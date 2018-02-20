package com.etsy.sahale


import java.util.Properties

import cascading.flow.Flow
import cascading.stats.CascadingStats
import cascading.stats.hadoop.HadoopStepStats

import java.io.IOException
import java.net.SocketException

import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.JavaConversions._

import spray.json._
import DefaultJsonProtocol._


object FlowStatus {
  val TEST_FLOW    = "com.etsy.sahale.TestFlow"
  val TEST_ID      = "BEEFCAFE1234"
  val EPSILON      = 2L * 86400L * 1000L // two days in millis is a safe bet
}

/**
 * Stores Flow-level metrics during a tracked job run.
 *
 * @author Eli Reisman
 */
class FlowStatus(val flow: Flow[_], props: Properties, jobArgs: Array[String], isTest: Boolean = false) {
  import com.etsy.sahale.FlowStatus._
  import com.etsy.sahale.FlowTracker.{UNKNOWN,NOT_LAUNCHED,NOT_YARN_JOB}

  def this(flow: Flow[_], props: Properties) = this(flow, props, Array.empty[String])

  val state = mutable.Map[String, Any](
    "flow_id"                   -> (if (isTest) TEST_ID   else flow.getID),
    "flow_name"                 -> (if (isTest) TEST_FLOW else flow.getName),
    "jt_url"                    -> UNKNOWN,
    "user_name"                 -> UNKNOWN,
    "flow_status"               -> NOT_LAUNCHED,
    "total_stages"              -> 0,
    "flow_duration"             -> 0L,
    "flow_priority"             -> 0,
    "cascade_id"                -> UNKNOWN,
    "yarn_job_history"          -> NOT_YARN_JOB,
    "hdfs_working_dir"          -> UNKNOWN,
    "flow_start_epoch_ms"       -> 0L,
    "flow_submit_epoch_ms"      -> 0L,
    "flow_end_epoch_ms"         -> 0L,
    "flow_links"                -> UNKNOWN,
    "aggregated"                -> Map.empty[String, Any],
    "config_props"              -> Map.empty[String, String]
  )

  def send: JsValue = JsonUtil.toJsonMap(state.toMap).toJson

  // let caller grab typed info from internal cache
  def get[T](key: String): T = state(key).asInstanceOf[T]

  // let caller pull cached, aggregated step data without recalc
  def getAggregate[T](key: String, default: T): T = {
    get[Map[String, Any]]("aggregated").getOrElse(key, default).asInstanceOf[T]
  }

  /**
   * Populates a map of up-to-date Flow properties to push to server.
   */
  def update: Unit = state += (
    "flow_id"              -> flow.getID,
    "flow_name"            -> flow.getName,
    "jt_url"               -> getResourceManager,
    "user_name"            -> getUsername,
    "tmp_dir"              -> getTempDirectory,
    "flow_status"          -> flow.getFlowStats.getStatus.toString,
    "total_stages"         -> flow.getFlowStats.getStepsCount, // Int
    "flow_duration"        -> updateFlowDuration, // Long (milliseconds)
    "flow_priority"        -> flow.getSubmitPriority, // Int
    "cascade_id"           -> getCascadeId,
    "yarn_job_history"     -> getHistoryServer, // host:port for YARN log links
    "hdfs_working_dir"     -> getHdfsWorkingDir,
    "flow_start_epoch_ms"  -> flow.getFlowStats.getStartTime, // Long
    "flow_submit_epoch_ms" -> flow.getFlowStats.getSubmitTime, // Long
    "flow_end_epoch_ms"    -> flow.getFlowStats.getFinishedTime, // Long
    "flow_links"           -> getFlowLinks,
    "aggregated"           -> aggregate,
    "config_props"         -> updateFlowConfigProps
  )

  def registerAggregators(in: Map[String, () => Any]): Unit = aggregators ++= in


  ///////////////// Utility methods ////////////////
  private def aggregate: Map[String, Any] = aggregators.foldLeft(Map.empty[String, Any]) {
    (acc, entry) => acc ++ Map(entry._1 -> entry._2())
  }

  private val aggregators = mutable.Map.empty[String, () => Any]

  private def updateFlowConfigProps: Map[String, String] = {
    flowPropsToExtract.foldLeft(Map.empty[String, String]) {
      (map: Map[String, String], prop: String) => flow.getProperty(prop) match {
        case s: String if (null != s) => map ++ Map(prop -> s)
        case _                        => map ++ Map(prop -> UNKNOWN)
      }
    }
  }

  private def getFlowLinks: String = flow.getProperty("sahale.flow.links") match {
    case s: String => s
    case _         => UNKNOWN
  }

  private def getCascadeId: String = flow.getCascadeID match {
    case s: String => s
    case _         => UNKNOWN
  }

  private def getHdfsWorkingDir: String = flow.getProperty("mapreduce.job.working.dir") match {
    case wd: String => wd
    case _          => UNKNOWN
  }

  private def getUsername: String = flow.getProperty("sahale.custom.user.name") match {
    case s: String => s
    case _         => System.getProperty("user.name")
  }

  private def getResourceManager: String = {
    def parseAddress(rm: String): String = if (rm.indexOf(":") > 0) { rm.substring(0, rm.indexOf(":")) } else { rm }

    (flow.getProperty("yarn.resourcemanager.webapp.address"), flow.getProperty("mapred.job.tracker")) match {
      case (rm: String, _) if null != rm => parseAddress(rm)
      case (_, jt: String) if null != jt => parseAddress(jt)
      case (_, _)                        => UNKNOWN
    }
  }

  private def getHistoryServer: String = {
    flow.getProperty("mapreduce.jobhistory.webapp.address") match {
      case historyServer: String => historyServer
      case _                     => NOT_YARN_JOB
    }
  }

  private def getTempDirectory: String = {
    flow.getProperty("hadoop.tmp.dir") match {
      case dir: String => dir
      case _           => "/tmp/cache"
    }
  }

  // sometimes Cascading reports the current epoch_ms rather than job duration
  private def updateFlowDuration: Long = {
    val fetched = math.max(flow.getFlowStats.getCurrentDuration, 0L)
    val test = math.abs(System.currentTimeMillis - fetched)
    if (test < EPSILON) 0L else fetched
  }

  private val flowPropsToExtract = props.getProperty("sahale.flow.selected.configs", "")
    .split(",").map { _.trim }.filter { _ != "" }.toSeq
}
