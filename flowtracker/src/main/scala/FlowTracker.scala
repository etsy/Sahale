package com.etsy.sahale

import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}
import org.apache.commons.httpclient.cookie.CookiePolicy
import org.apache.commons.httpclient.methods.{StringRequestEntity, PostMethod}
import org.apache.http.client.params.ClientPNames
import org.apache.log4j.Logger

import cascading.flow.Flow

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Properties

import scala.util.Try
import scala.collection.mutable
import scala.collection.JavaConversions._

import spray.json._
import DefaultJsonProtocol._

object FlowTracker {
  type AggFunc = () => Any

  val PROPSFILE = "flow-tracker.properties"

  val NOT_LAUNCHED = "NOT_LAUNCHED"
  val NOT_YARN_JOB = "false"
  val UNKNOWN = "UNKNOWN"

  val UPDATE_FLOW = "flow/update"
  val UPDATE_STEPS = "steps/update"
  val INSERT_AGG = "agg/update"
  val CREATE_EDGES= "edges/update"

  val REFRESH_INTERVAL_MS = 8 * 1000

  val CheckIsCascadingFlowId = """([A-Fa-f0-9]+)""".r

  val HTTP_CONNECTION_TIMEOUT = 5 * 60 * 1000;
  val HTTP_SOCKET_TIMEOUT = 5 * 60 * 1000;

  val LOG: Logger = Logger.getLogger(classOf[FlowTracker])

  val props = getTrackerProperties

  private var client: HttpClient = getHttpClient()

  // master map of steps' hadoop counters to be aggregated and added to the flow data
  private val StepCounterAggregators = Map[String, (String, String)](
    "flow_hdfs_bytes_read"      -> ("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ"),
    "flow_file_bytes_read"      -> ("org.apache.hadoop.mapreduce.FileSystemCounter", "FILE_BYTES_READ"),
    "flow_gs_bytes_read"        -> ("org.apache.hadoop.mapreduce.FileSystemCounter", "GS_BYTES_READ"),
    "flow_hdfs_bytes_written"   -> ("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN"),
    "flow_file_bytes_written"   -> ("org.apache.hadoop.mapreduce.FileSystemCounter", "FILE_BYTES_WRITTEN"),
    "flow_gs_bytes_written"     -> ("org.apache.hadoop.mapreduce.FileSystemCounter", "GS_BYTES_WRITTEN"),

    "flow_total_map_tasks"      -> ("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_MAPS"),
    "flow_total_reduce_tasks"   -> ("org.apache.hadoop.mapreduce.JobCounter", "TOTAL_LAUNCHED_REDUCES"),
    "flow_map_vcore_millis"     -> ("org.apache.hadoop.mapreduce.JobCounter", "VCORES_MILLIS_MAPS"),
    "flow_reduce_vcore_millis" -> ("org.apache.hadoop.mapreduce.JobCounter", "VCORES_MILLIS_REDUCES"),

    "flow_shuffled_maps"        -> ("org.apache.hadoop.mapreduce.TaskCounter", "SHUFFLED_MAPS"),
    "flow_map_records_out"      -> ("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS"),
    "flow_reduce_records_in"    -> ("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS"),
    "flow_reduce_records_out"   -> ("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS"),
    "flow_map_bytes_out"        -> ("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_BYTES"),
    "flow_merged_map_outputs"   -> ("org.apache.hadoop.mapreduce.TaskCounter", "MERGED_MAP_OUTPUTS"),
    "flow_reduce_shuffle_bytes" -> ("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_SHUFFLE_BYTES"),
    "flow_cpu_millis"           -> ("org.apache.hadoop.mapreduce.TaskCounter", "CPU_MILLISECONDS"),
    "flow_gc_millis"            -> ("org.apache.hadoop.mapreduce.TaskCounter", "GC_TIME_MILLIS"),
    "flow_spilled_records"      -> ("org.apache.hadoop.mapreduce.TaskCounter", "SPILLED_RECORDS"),
    "flow_heap_committed_bytes" -> ("org.apache.hadoop.mapreduce.TaskCounter", "COMMITTED_HEAP_BYTES"),
    "flow_virtual_mem_bytes"    -> ("org.apache.hadoop.mapreduce.TaskCounter", "VIRTUAL_MEMORY_BYTES"),
    "flow_physical_mem_bytes"   -> ("org.apache.hadoop.mapreduce.TaskCounter", "PHYSICAL_MEMORY_BYTES"),

    "flow_tuples_read"          -> ("cascading.flow.StepCounters", "Tuples_Read"),
    "flow_tuples_written"       -> ("cascading.flow.StepCounters", "Tuples_Written")
    // counter metrics set here will be aggregated and published in flow status updates
  )

  def getDefaultHostPort = props("host").trim + ":" + props("port").trim

  private def getTrackerProperties: Properties = {
    val props = new Properties();
    props.load(Thread.currentThread.getContextClassLoader.getResourceAsStream(PROPSFILE))
    props
  }

  def getHttpClient(
                     httpConnectionTimeout: Int = HTTP_CONNECTION_TIMEOUT,
                     httpSocketTimeout: Int = HTTP_SOCKET_TIMEOUT) = client match {
    case hc: HttpClient if (null != hc) => hc
    case _                                    =>
      client = new HttpClient(new MultiThreadedHttpConnectionManager)
      client.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.BROWSER_COMPATIBILITY);
      client.getHttpConnectionManager().getParams().setConnectionTimeout(httpConnectionTimeout);
      client.getHttpConnectionManager().getParams().setSoTimeout(httpSocketTimeout);
      client
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
                  val disableProgressBar: Boolean,
                  val httpConnectionTimeout: Int = FlowTracker.HTTP_CONNECTION_TIMEOUT,
                  val httpSocketTimeout: Int = FlowTracker.HTTP_SOCKET_TIMEOUT) extends java.lang.Runnable {
  import com.etsy.sahale.FlowTracker._

  // mutable because we have to build this mapping as we go after run() is called
  val stepStatusMap = mutable.Map[String, StepStatus]()
  val edgeMap = mutable.Map[String, Set[Int]]()

  // manages global job state for this run
  val flowStatus = new FlowStatus(flow, FlowTracker.props)

  // so that we can compose a chain of multiple strategies, end users might
  // already have FlowStepStrategy implementations they need to apply later
  flow.setFlowStepStrategy(
    FlowStepStrategies.plus(flow.getFlowStepStrategy, new FlowTrackerStepStrategy(stepStatusMap))
  )

  // Store the server's host:port so that subclasses have access to it
  val serverHostPort = if(hostPort.trim.isEmpty) getDefaultHostPort else hostPort.trim

  // Constructors below are for easy Java interop with FlowTracker
  def this(flow: Flow[_], runCompleted: AtomicBoolean, hostPort: String, httpConnectionTimeout: Int, httpSocketTimeout: Int) =
    this(flow, runCompleted, hostPort, false, httpConnectionTimeout, httpSocketTimeout)

  def this(flow: Flow[_], runCompleted: AtomicBoolean, hostPort: String) =
    this(flow, runCompleted, hostPort, FlowTracker.HTTP_CONNECTION_TIMEOUT, FlowTracker.HTTP_SOCKET_TIMEOUT)

  def this(flow: Flow[_], runCompleted: AtomicBoolean) =
    this(flow, runCompleted, "", false, FlowTracker.HTTP_CONNECTION_TIMEOUT, FlowTracker.HTTP_SOCKET_TIMEOUT)

  override def run(): Unit = {
    var isTrackable: Boolean = true
    Try {
      initializeTrackedJobState
    }.recover {
      case t: Throwable =>
        LOG.warn("""
          Failed to initialize FlowTracker.  This is NOT a fatal error.
          The job will run as normal, but it will not be tracked by Sahale.
          """.stripMargin.trim, t)
        runCompleted.set(true)
        isTrackable = false
    }

    val maxFailures = 10
    var numFailures = 0
    while(!runCompleted.get && isTrackable) {
      Try {
          updateSteps
          updateFlow
          updateAggregates
          if (!disableProgressBar) {
            logFlowStatus
          }
        }.recover {
          case t: Throwable if 1 + numFailures < maxFailures=>
            LOG.warn(s"""
              FlowTracker has thrown an exception because an update iteration
              failed. This is NOT a fatal error. We will skip this update
              iteration. ${maxFailures - numFailures} attempts remain.
              """.stripMargin.trim, t)

            numFailures += 1

          case t: Throwable =>
            LOG.warn("""
              FlowTracker for this run has thrown an exception. This is NOT a
              fatal error. The run will complete as normal, but the remainder
              will not be tracked by Sahale.
              """.stripMargin.trim, t)
            runCompleted.set(true)
        }

        sleep(REFRESH_INTERVAL_MS)
    }

    // Push the final updates, but only if we have not hit our failure limit
    if(numFailures < maxFailures && isTrackable) {
      Try {
        updateSteps
        updateFlow
        updateAggregates
      }
    }
  }

  def updateFlow: Unit = { flowStatus.update ; pushFlowReport }

  // only update & push step data that's changed since last push
  def updateSteps: Unit = pushStepReport(
    stepStatusMap.foldLeft(Map.empty[String, StepStatus]) { (next, entry) =>
      val old = entry._2.get[String]("step_status")
      val current = entry._2.getStatus
      (old, current) match {
        case (o, c) if (c == "RUNNING" || o != c) => entry._2.update ; next + entry
        case _                                    => next
      }
    }
  )

  def updateAggregates: Unit = {
    import spray.json._
    import DefaultJsonProtocol._

    val agg = Map[String, Any](
      "flow_id"  -> flow.getID,
      "epoch_ms" -> System.currentTimeMillis,
      "agg_json" -> flowStatus.get[Map[String, Any]]("aggregated")
    )
    val json = com.etsy.sahale.JsonUtil.toJsonMap(agg).toJson.compactPrint

    pushReport(sahaleUrl(INSERT_AGG), json)
  }

  /**
   * Init our flow, edge, and stage maps since we can't until run() is called.
   */
  def initializeTrackedJobState: Unit = {
    new FlowGraphBuilder(flow, stepStatusMap, edgeMap)

    flowStatus.registerAggregators(
      // add the progress aggregator func
      Map[String, AggFunc]("flow_progress" -> calculateFlowProgress) ++
      // add all counter-based aggregator functions
      StepCounterAggregators.map { case(k,v) => agg(k, v) }
        .foldLeft(Map.empty[String, AggFunc]) { (acc, next) => acc ++ next }
    )

    pushEdgeReport                      // only sent once
    pushStepReport(stepStatusMap.toMap) // first time send all, after just changeset
    pushFlowReport                      // sent every update
    updateAggregates                    // sent every update

    println(Console.REVERSED + "Follow your running job's progress from your browser: " + sahaleUrl(s"flowgraph/${flow.getID}") + Console.RESET)
  }

  def sleep(millis: Long): Unit = try { Thread.sleep(millis) } catch { case _: Exception => }


  /////////////////// Utility functions for aggregating Step data for Flow updates /////////////////
  def sumMapProgress: (Double, Int) = stepStatusMap.values.foldLeft( (0.0, 0) ) {
    (acc: (Double, Int), step: StepStatus) => (acc._1 + step.get[Double]("map_progress"), acc._2 + 1)
  }

  def sumReduceProgress: (Double, Int) = stepStatusMap.values.foldLeft( (0.0, 0) ) {
    (acc: (Double, Int), step: StepStatus) => step.get[Boolean]("has_reduce_stage") match {
      case true => (acc._1 + step.get[Double]("reduce_progress"), acc._2 + 1)
      case _    => (acc._1, acc._2)
    }
  }

  def calculateFlowProgress(): Any = {
    val mapProg = sumMapProgress
    val reduceProg = sumReduceProgress
    val subtotal = ((mapProg._1 + reduceProg._1) / (mapProg._2 + reduceProg._2))
    ("%3.2f" format subtotal).toDouble
  }

  def agg(name: String, groupAndKey: (String, String)): Map[String, AggFunc] = Map[String, AggFunc](
    name -> (() => stepStatusMap.values.foldLeft(0L) {
      (sum: Long, step: StepStatus) => step.aggrFunc(groupAndKey._1, groupAndKey._2) + sum
    })
  )

  /////////////////// Utiilty functions for pushing data to Sahale server /////////////////
  def pushFlowReport: Int = pushReport(sahaleUrl(UPDATE_FLOW), flowStatus.send.compactPrint)

  def pushEdgeReport: Int = pushReport(sahaleUrl(CREATE_EDGES), edgeMap.toMap.toJson.compactPrint)

  def pushStepReport(steps: Map[String, StepStatus]): Int = steps.keys.size match {
    case none: Int if (none == 0) =>
      //LOG.info("No new FlowStep updates to push to Sahale")
      none
    case count =>
      //LOG.info(s"Pushing $count FlowStep updates to Sahale")
      pushReport(sahaleUrl(UPDATE_STEPS), steps.map { case(k, v) => v.send }.toJson.compactPrint)
  }

  def setAdditionalHeaders: Map[String, String] = {
    // To be overridden by child classes, e.g. to set authentication headers
    Map.empty
  }

  def pushReport(uri: String, json: String): Int = {
    flow.getID match {
      case CheckIsCascadingFlowId(id) => {
        val url = uri + "/" + id + "?method=POST"
        // LOG.info("REQUEST URL TO API:" + url)
        val request = new PostMethod(url)
        val entity  = new StringRequestEntity(json, "application/json", "UTF-8")
        try {
          request.setRequestEntity(entity)
          setAdditionalHeaders.foreach { case (headerName, headerValue) =>
            request.addRequestHeader(headerName, headerValue)
          }
          val code = getHttpClient(httpConnectionTimeout, httpSocketTimeout).executeMethod(request)
          code
        } catch {
          case e: IOException =>
            logRequestResponse(url, request, json)
            throw e
        } finally {
          if (null != request) {
            try {
              val asStream = request.getResponseBodyAsStream
              if (asStream != null)
                asStream.close()
            } catch {
              case e: IOException => LOG.warn("Could not close response stream for [" + url + "]", e)
            }
            request.releaseConnection
          }
        }
      }
      case _ => {
        LOG.warn(s"Bad FlowID: ${flow.getID}")
        -1 // error
      }
    }
  }

  def logRequestResponse(url: String, response: PostMethod, json: String): Unit = {
    LOG.info(s"Sent JSON to $url:\n${json}")
    if (response.getStatusLine != null) {
      LOG.info(s"Response status code: ${response.getStatusCode}")
      LOG.info(s"Response status line: ${response.getStatusLine}")
      LOG.info(s"Response status text: ${response.getStatusText}")
    } else {
      LOG.info("No response status to debug")
    }
    LOG.info(s"Response x-error-detail: ${response.getResponseHeader("x-error-detail")}")
  }

  def sahaleUrl(suffix: String = ""): String = s"${serverHostPort}/${suffix}"

  /////////////////// Utility functions for console progress bar /////////////////
  def logFlowStatus: Unit = {
    val flowProgress = flowStatus.getAggregate[Double]("flow_progress", 0.0)
    val flowDuration = flowStatus.get[Long]("flow_duration")

    val arrows = (20 * (flowProgress / 100.0)).toInt
    val progressBar = Console.WHITE + "[" + Console.YELLOW + (">" * arrows) + Console.WHITE + (" " * (20 - arrows)) + "]"
    val progressIndicator = "Job Status: " + getColoredFlowStatus + "\tProgress: " + ("%3.2f" format flowProgress) + "% "

    print("\u000D" + (" " * 60));
    print("\u000D" + Console.WHITE + progressIndicator + progressBar +
      "\t Running " + (flowDuration / 1000L) + "secs" + Console.RESET + (" " * 10));
  }

  def getColoredFlowStatus: String = {
    val statusColor = flow.getFlowStats.getStatus.toString match {
      case "SUCCESSFUL" | "RUNNING" => Console.GREEN
      case "FAILED" => Console.RED
      case _ => Console.WHITE
    }
    statusColor + flow.getFlowStats.getStatus.toString + Console.WHITE
  }
}
