package com.etsy.sahale


import cascading.flow.Flow
import cascading.stats.CascadingStats
import cascading.stats.hadoop.HadoopStepStats

import java.io.IOException
import java.net.SocketException

import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.JavaConversions._


object FlowStatus {
  def initial: Map[String, String] = Map(
    "jt_url"                    -> FlowTracker.UNKNOWN,
    "user_name"                 -> System.getProperty("user.name"),
    "flow_status"               -> "NOT_LAUNCHED",
    "total_stages"              -> "0",
    "flow_progress"             -> "0.00",
    "flow_duration"             -> "0",
    "flow_hdfs_bytes_written"   -> "0",
    "flow_priority"             -> "0",
    "cascade_id"                -> FlowTracker.UNKNOWN,
    "yarn_job_history"          -> FlowTracker.NOT_YARN_JOB,
    "hdfs_working_dir"          -> FlowTracker.UNKNOWN,
    "flow_start_epoch_ms"       -> "0",
    "flow_submit_epoch_ms"      -> "0",
    "flow_end_epoch_ms"         -> "0"
  )
}

/**
 * Stores Flow-level metrics during a tracked job run.
 *
 * @author Eli Reisman
 */
class FlowStatus(val flow: Flow[_]) {
  // these are updated in the FlowTracker using StepStatus rollups
  var flowProgress = "0.00"
  var flowHdfsBytesWritten = "0"

  /**
   * Populates a map of up-to-date Flow properties to push to server.
   */
  def toMap: Map[String, String] = Map(
    "jt_url"                  -> getJobTrackerFromFlowProps,
    "user_name"               -> System.getProperty("user.name", FlowTracker.UNKNOWN),
    "flow_status"             -> flow.getFlowStats.getStatus.toString,
    "total_stages"            -> flow.getFlowStats.getStepsCount.toString,
    "flow_progress"           -> flowProgress,
    "flow_duration"           -> updateFlowDuration, // milliseconds
    "flow_hdfs_bytes_written" -> flowHdfsBytesWritten,
    "flow_priority"           -> flow.getSubmitPriority.toString,
    "cascade_id"              -> flow.getCascadeID,
    "yarn_job_history"        -> getHistoryServerFromFlowProps, // host:port for YARN log links
    "hdfs_working_dir"        -> getHdfsWorkingDir,
    "flow_start_epoch_ms"     -> flow.getFlowStats.getStartTime.toString,
    "flow_submit_epoch_ms"    -> flow.getFlowStats.getSubmitTime.toString,
    "flow_end_epoch_ms"       -> flow.getFlowStats.getFinishedTime.toString
  )

  def getHdfsWorkingDir: String = {
    flow.getProperty("mapreduce.job.working.dir") match {
      case wd: String => wd
      case _          => FlowTracker.UNKNOWN
    }
  }

  def getJobTrackerFromFlowProps: String = {
    flow.getProperty("mapred.job.tracker") match {
      case jt: String => if (jt.indexOf(":") > 0) { jt.substring(0, jt.indexOf(":")) } else { jt }
      case _          => FlowTracker.UNKNOWN
    }
  }

  def getHistoryServerFromFlowProps: String = {
    flow.getProperty("mapreduce.jobhistory.webapp.address") match {
      case historyServer: String => historyServer
      case _ => FlowTracker.NOT_YARN_JOB
    }
  }

  def updateFlowDuration: String = {
    flow.getFlowStats.getFlowStepStats.toList.map {
      fss => fss.getCurrentDuration
    }.max.toString
  }
}
