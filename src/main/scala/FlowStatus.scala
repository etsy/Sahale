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
    "jt_url" -> FlowTracker.UNKNOWN,
    "user_name" -> System.getProperty("user.name"),
    "flow_status" -> "NOT_LAUNCHED",
    "total_stages" -> "0",
    "flow_progress" -> "0.00",
    "flow_duration" -> "0",
    "flow_hdfs_bytes_written" -> "0",
    "yarn_job_history" -> FlowTracker.NOT_YARN_JOB
  )
}

/**
 * Fields used in SQL queries are not stored here, this will be a lump of JSON
 * that is extracted and used to compose views on the server side. This also makes
 * it fairly transparent to change/expand the data model in the future.
 */
class FlowStatus(val flow: Flow[_]) {
  // these are updated in the FlowTracker using StepStatus rollups
  var flowProgress = "0.00"
  var flowHdfsBytesWritten = "0"

  /**
   * Populates a map of up-to-date Flow properties to push to server.
   */
  def toMap: Map[String, String] = Map(
    "jt_url" -> getJobTrackerFromFlowProps, // JT URL if applicable, for job logs
    "user_name" -> System.getProperty("user.name"),
    "flow_status" -> getFlowStatus, // status of entire job
    "total_stages" -> getTotalStepCount, // total stages in this job
    "flow_progress" -> flowProgress,
    "flow_duration" -> updateFlowDuration, // milliseconds
    "flow_hdfs_bytes_written" -> flowHdfsBytesWritten,
    "yarn_job_history" -> getHistoryServerFromFlowProps // host:port for YARN log links
  )

  def getFlowStatus: String = {
    flow.getFlowStats.getStatus.toString
  }

  def getTotalStepCount: String = {
    flow.getFlowStats.getStepsCount.toString
  }

  def getJobTrackerFromFlowProps: String = {
    flow.getProperty("mapred.job.tracker") match {
      case jt: String => if (jt.indexOf(":") > 0) { jt.substring(0, jt.indexOf(":")) } else { jt }
      case _ => FlowTracker.UNKNOWN
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
