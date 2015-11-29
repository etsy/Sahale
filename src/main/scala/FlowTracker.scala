package com.etsy.sahale

import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager, Header}
import org.apache.commons.httpclient.cookie.CookiePolicy
import org.apache.commons.httpclient.methods.PostMethod
import org.apache.hadoop.mapred.{JobConf, JobClient}
import org.apache.http.client.params.ClientPNames
import org.apache.log4j.Logger

import cascading.flow.{Flow, FlowStep, FlowStepStrategy}
import cascading.flow.hadoop.HadoopFlowStep
import cascading.stats.{CascadingStats, FlowStepStats}
import cascading.stats.hadoop.HadoopStepStats
import cascading.tuple.Fields

import java.io.IOException
import java.net.SocketException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Properties

import scala.collection.mutable
import scala.collection.JavaConversions._

import spray.json._
import DefaultJsonProtocol._

object FlowTracker {
  val PROPSFILE = "flow-tracker.properties"

  val NOT_YARN_JOB = "false"
  val INITIAL_FLOW_STATE = "NOT_LAUNCHED"
  val UNKNOWN = "UNKNOWN"

  val UPDATE_FLOW = "flow/update"
  val UPDATE_STEPS = "steps/update"
  val CREATE_EDGES= "edges/update"

  val REFRESH_INTERVAL_MS = 8 * 1000

  val CheckIsCascadingFlowId = """([A-Fa-f0-9]+)""".r

  val LOG: Logger = Logger.getLogger(classOf[FlowTracker])

  val props = getTrackerProperties

  def getDefaultHostPort = props("host").trim + ":" + props("port").trim

  private def getTrackerProperties: Properties = {
    val props = new Properties();
    props.load(Thread.currentThread.getContextClassLoader.getResourceAsStream(PROPSFILE))
    props
  }
}


/**
 * Poll running Cascading Flow for metrics and push to Sahale server configued in
 * <code>src/main/resources/flow-tracker.properties</code>
 *
 * @author Eli Reisman
 */
class FlowTracker(val flow: Flow[_],
                  val runCompleted: AtomicBoolean,
                  val hostPort: String,
                  val disableProgressBar: Boolean) extends java.lang.Runnable {
  import com.etsy.sahale.FlowTracker._

  // mutable because we have to build this mapping as we go after run() is called
  val stepStatusMap = mutable.Map[String, StepStatus]()

  val client = getHttpClient

  // manages global job state for this run
  val flowStatus = new FlowStatus(flow, FlowTracker.props)

  // so that we can compose a chain of multiple strategies, end users might
  // already have FlowStepStrategy implementations they need to apply later
  flow.setFlowStepStrategy(
    FlowStepStrategies.plus(flow.getFlowStepStrategy, new FlowTrackerStepStrategy(stepStatusMap))
  )

  // Constructors below are for easy Java interop with FlowTracker
  def this(flow: Flow[_], runCompleted: AtomicBoolean, hostPort: String) = this(flow, runCompleted, hostPort, false)

  def this(flow: Flow[_], runCompleted: AtomicBoolean) = this(flow, runCompleted, "", false)

  override def run(): Unit = {
    try {
      initializeTrackedJobState
      registerShutdownHook

      while (!runCompleted.get) {
        updateSteps
        updateFlow
        if (!disableProgressBar) {
          logFlowStatus
        }
        try { Thread.sleep(REFRESH_INTERVAL_MS) } catch { case _: Exception => }
      }

    } catch {
      case t: Throwable => {
        LOG.warn("FlowTracker for this run has thrown an exception. " +
          "The run will complete as normal, but the remainder will not be tracked.", t)
        runCompleted.set(true);
      }
    } finally {
      updateSteps
      updateFlow
      if (null != client) {
        client.getHttpConnectionManager.asInstanceOf[MultiThreadedHttpConnectionManager].shutdown
      }
    }
  }

  /**
   * Register a shutdown hook to perform the final update
   */
  def registerShutdownHook: Unit = {
    val shutdownHookWorker = new Thread(new FlowTrackerShutdownHookWorker)
    Runtime.getRuntime.addShutdownHook(shutdownHookWorker)
  }

  /**
   * Init our flow, edge, and stage maps since we can't until run() is called.
   */
  def initializeTrackedJobState: Unit = {
    // push initial flow status report to server
    pushFlowReport(flow, INITIAL_FLOW_STATE, FlowStatus.initial)
    val edgeMap = (new FlowGraphBuilder(flow, stepStatusMap)).composeDag
    pushReport(flow.getID, sahaleUrl(CREATE_EDGES), edgeMap)
    pushStepReport(flow.getID, stepStatusMap.toMap)
    println(Console.REVERSED + "Follow your running job's progress from your browser: " + sahaleUrl() + Console.RESET)
  }


  /////////////////// Utility functions for aggregating Step data for Flow updates /////////////////
  def updateFlow: Unit = {
    flowStatus.flowProgress = calculateFlowProgress
    flowStatus.flowHdfsBytesWritten = sumHdfsBytesWritten

    pushFlowReport(flow, flow.getFlowStats.getStatus.toString, flowStatus.toMap)
  }

  def updateSteps: Unit = pushStepReport(flow.getID,
    flow.getFlowSteps.toList.foldLeft(Map.empty[String, StepStatus]) {
      (next: Map[String, StepStatus], fs: FlowStep[_]) =>
        val hfs: HadoopFlowStep = fs.asInstanceOf[HadoopFlowStep]
        val conf = hfs.getConfig
        val id = hfs.getID
        val oldStatus = stepStatusMap(id).stepStatus
        val newStatus = hfs.getFlowStepStats.getStatus.toString
        (oldStatus, newStatus) match {
          case (o, n) if (n == "RUNNING" || o != n) => {
            stepStatusMap(id).update(hfs)
            stepStatusMap(id).setConfigurationProperties(conf)
            next ++ Map(id -> stepStatusMap(id))
          }
          case _  => next
        }
    }
  )

  def sumHdfsBytesWritten: String = {
    stepStatusMap.keys.foldLeft(0L) { (sum: Long, stageId: String) =>
      sum + stepStatusMap(stageId).hdfsBytesWritten
    }.toString
  }

  def sumMapProgress: (Double, Int) = {
    stepStatusMap.keys.foldLeft( (0.0, 0) ) { (acc: (Double, Int), stageId: String) =>
      (acc._1 + stepStatusMap(stageId).mapProgress.toDouble, acc._2 + 1)
    }
  }

  def sumReduceProgress: (Double, Int) = {
    stepStatusMap.keys.foldLeft( (0.0, 0) ) { (acc: (Double, Int), stageId: String) =>
      val step = stepStatusMap(stageId)
      step.hasReduceStage match {
        case true => (acc._1 + step.reduceProgress.toDouble, acc._2 + 1)
        case _    => (acc._1, acc._2)
      }
    }
  }

  def calculateFlowProgress: String = {
    val mapProg = sumMapProgress
    val reduceProg = sumReduceProgress
    "%3.2f" format ((mapProg._1 + reduceProg._1) / (mapProg._2 + reduceProg._2))
  }

  /////////////////// Utiilty functions for pushing data to Sahale server /////////////////
  def getHttpClient = {
    val c = new HttpClient(new MultiThreadedHttpConnectionManager)
    c.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BROWSER_COMPATIBILITY);
    c
  }

  def pushReport(flowId: String, uri: String, map: Map[String, String]): Int = {
    flowId match {
      case CheckIsCascadingFlowId(id) => {
        val url = uri + "/" + flowId + "?method=POST"
        // LOG.info("REQUEST URL TO API:" + url)
        val request = new PostMethod(url)
        try {
          request.setRequestHeader("Content-Type", "application/x-www-form-urlencoded")
          map.map { entry => request.addParameter(new Header(entry._1, entry._2)) }
          val statusCode = client.executeMethod(request)
          request.getResponseBodyAsStream.close
          //logMap(map, request); // For DEBUGGING
          statusCode
        } finally {
          if (null != request) {
            request.releaseConnection
          }
        }
      }
      case _ => {
        LOG.warn("Unable to locate flow_id for this job report. Request URI: " + uri)
        -1 // error
      }
    }
  }

  def logMap(map: Map[String, String], response: PostMethod): Unit = {
    LOG.info("PUSHED MAP:\n" + map.mkString("\n"))
    LOG.info("WITH RESPONSE STATUS CODE: " + response.getStatusCode)
    LOG.info("WITH RESPONSE x-error-detail: " + response.getResponseHeader("x-error-detail"))
    LOG.info("WITH RESPONSE STATUS LINE: " + response.getStatusLine)
    LOG.info("WITH RESPONSE STATUS TEXT: " + response.getStatusText)
  }

  def sahaleUrl(suffix: String = ""): String = {
    val path = "/" + suffix
    hostPort match {
      case ""         => getDefaultHostPort + path
      case hp: String => hp.trim + path
    }
  }

  def pushFlowReport(flow: Flow[_], flowStatus: String, map: Map[String, String]): Int = {
    val sendMap: Map[String, String] = Map(
      "flowname" -> flow.getName,
      "flowstatus" -> flowStatus,
      "json" -> java.net.URLEncoder.encode(
        map.map {
          case(k, v) => (k, if (null == v) JsNull else JsString(v))
        }.asInstanceOf[Map[String, JsValue]].toJson.compactPrint,
        "UTF-8"
      )
    )
    pushReport(flow.getID, sahaleUrl(UPDATE_FLOW), sendMap)
  }

  def pushStepReport(flowId: String, steps: Map[String, StepStatus]): Int = {
    steps.keys.size match {
      case none: Int if (none == 0) => LOG.info("No new FlowStep updates to push to Sahale") ; none
      case count => {
        LOG.debug("Pushing " + count + " FlowStep updates to Sahale")
        val sendMap: Map[String, String] = steps.map {
          step => step._1 -> java.net.URLEncoder.encode(step._2.jsonMap, "UTF-8")
        }.toMap
        pushReport(flowId, sahaleUrl(UPDATE_STEPS), sendMap)
      }
    }
  }


  /////////////////// Utility functions for console progress bar /////////////////
  def logFlowStatus: Unit = {
    val arrows = (20 * (flowStatus.flowProgress.toDouble / 100.0)).toInt
    val progressBar = Console.WHITE + "[" + Console.YELLOW + (">" * arrows) + Console.WHITE + (" " * (20 - arrows)) + "]"
    print("\u000D" + (" " * 60));
    print("\u000D" + Console.WHITE + "Job Status: " + getColoredFlowStatus + "\tProgress: " + flowStatus.flowProgress + "% " +
      progressBar + "\t Running " + (flowStatus.updateFlowDuration.toLong / 1000) + "secs" + Console.RESET + (" " * 10));
  }

  def getColoredFlowStatus: String = {
    val statusColor = flow.getFlowStats.getStatus.toString match {
      case "SUCCESSFUL" | "RUNNING" => Console.GREEN
      case "FAILED" => Console.RED
      case _ => Console.WHITE
    }
    statusColor + flow.getFlowStats.getStatus.toString + Console.WHITE
  }


  /**
   * There is an edge case in the tracking that occurs sometimes when the client JVM is terminated with a SIGTERM
   * Normally this would interrupt the tracking and prevent sending a final update
   * This could cause jobs to appear to stick around in a running state even though they have been killed
   * Registering a shutdown hook lets us perform a final update during the process of shutting down the JVM
   */
  private class FlowTrackerShutdownHookWorker extends Runnable {
    val tracker = FlowTracker.this

    override def run(): Unit = {
      if (!tracker.runCompleted.get && tracker.client != null) {
        LOG.info("Performing final update from shutdown hook")
        tracker.updateSteps
        tracker.updateFlow
        tracker.client.getHttpConnectionManager.asInstanceOf[MultiThreadedHttpConnectionManager].shutdown
      }
    }
  }
}
