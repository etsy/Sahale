package com.etsy.sahale

import org.apache.log4j.Logger

import cascading.flow.Flow
import cascading.flow.hadoop.HadoopFlowStep
import cascading.stats.{CascadingStats, FlowStepStats}
import cascading.stats.hadoop.{HadoopStepStats, HadoopSliceStats}
import cascading.tap.Tap
import cascading.util.Util

//import org.json.JSONObject
import com.codahale.jerkson.Json._

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Value class to store tracked per-step workflow data.
 *
 * @author Eli Reisman
 */
class StepStatus(val stepNumber: String, val stepId: String) {
  var sources = FlowTracker.UNKNOWN
  var sink = FlowTracker.UNKNOWN
  var sourcesFields = FlowTracker.UNKNOWN
  var sinkFields = FlowTracker.UNKNOWN
  var jobId = "NO_JOB_ID"
  var mapProgress = "0.00"
  var reduceProgress = "0.00"
  var stepStatus = "NOT_LAUNCHED"
  var stepRunningTime = "0"
  var mapTasks = "0"
  var reduceTasks = "0"
  var hdfsBytesRead = "0"
  var hdfsBytesWritten = "0"
  var fileBytesRead = "0"
  var fileBytesWritten = "0"
  var tuplesRead = "0"
  var tuplesWritten = "0"
  var dataLocalMapTasks = "0"
  var rackLocalMapTasks = "0"
  var committedHeapBytes = "0"
  var gcMillis = "0"
  var cpuMillis = "0"
  var ioReadMillis = "0"
  var ioWriteMillis = "0"
  var failedMapTasks = "0"
  var failedReduceTasks = "0"
  // CONSTRUCTOR ENDS HERE

  override def toString: String = {
    toMap.toString
  }

  def toMap: Map[String, String] = {
    Map(
      "stepnumber" -> stepNumber,
      "sources" -> sources,
      "sink" -> sink,
      "sourcesfields" -> sourcesFields,
      "sinkfields" -> sinkFields,
      "jobid" -> jobId,
      "stepid" -> stepId,
      "mapprogress" -> mapProgress,
      "reduceprogress" -> reduceProgress,
      "stepstatus" -> stepStatus,
      "steprunningtime" -> stepRunningTime,
      "maptasks" -> mapTasks,
      "reducetasks" -> reduceTasks,
      "hdfsbytesread" -> hdfsBytesRead,
      "hdfsbyteswritten" -> hdfsBytesWritten,
      "filebytesread" -> fileBytesRead,
      "filebyteswritten" -> fileBytesWritten,
      "tuplesread" -> tuplesRead,
      "tupleswritten" -> tuplesWritten,
      "datalocalmaptasks" -> dataLocalMapTasks,
      "racklocalmaptasks" -> rackLocalMapTasks,
      "committedheapbytes" -> committedHeapBytes,
      "gcmillis" -> gcMillis,
      "cpumillis" -> cpuMillis,
      "ioreadmillis" -> ioReadMillis,
      "iowritemillis" -> ioWriteMillis,
      "failedmaptasks" -> failedMapTasks,
      "failedreducetasks" -> failedReduceTasks
    )
  }

  def setSourcesAndSink(sources: String, sourcesFields: String, sink: String, sinkFields: String): Unit = {
    this.sources = sources
    this.sourcesFields = sourcesFields
    this.sink = sink
    this.sinkFields = sinkFields
  }

  /**
   * Updates the non-static step properties.
   */
  def update(flow: Flow[_], hadoopStepStats: HadoopStepStats): Unit = {
    jobId = hadoopStepStats.getJobID
    mapProgress = "%3.2f" format (hadoopStepStats.getMapProgress * 100.0)
    reduceProgress = getReduceProgress(hadoopStepStats)
    stepRunningTime = getStepRunningTime(hadoopStepStats) // checks old stepStatus value - MUST be updated first!
    stepStatus = hadoopStepStats.getStatus.toString
    mapTasks = getMapperCount(hadoopStepStats)
    reduceTasks = getReducerCount(hadoopStepStats)
    hdfsBytesRead = getHdfsBytesRead(hadoopStepStats)
    hdfsBytesWritten = getHdfsBytesWritten(hadoopStepStats)
    fileBytesRead = getFileBytesRead(hadoopStepStats)
    fileBytesWritten = getFileBytesWritten(hadoopStepStats)
    tuplesRead = getTuplesRead(hadoopStepStats)
    tuplesWritten = getTuplesWritten(hadoopStepStats)
    dataLocalMapTasks = getLocalMapperCount(hadoopStepStats)
    rackLocalMapTasks = getRackMapperCount(hadoopStepStats)
    committedHeapBytes = getCommittedHeapBytes(hadoopStepStats)
    gcMillis = getGcTimeMillis(hadoopStepStats)
    cpuMillis = getCpuTimeMillis(hadoopStepStats)
    ioReadMillis = getIoReadTimeMillis(hadoopStepStats)
    ioWriteMillis = getIoWriteTimeMillis(hadoopStepStats)
    failedMapTasks = getFailedMapperCount(hadoopStepStats)
    failedReduceTasks = getFailedReducerCount(hadoopStepStats)
  }

  // Calling captureDetail every update just for this is not worth it.
  // Just be sure final update of run picks up accurate stats for this.
  def getStepRunningTime(hss: HadoopStepStats): String = {
    val forceUpdate: Boolean = (stepStatus == "RUNNING" && hss.getStatus.toString != "RUNNING")
    val diff = forceUpdate match {
      case true => {
        try {
          hss.captureDetail // ain't cheap, keep an eye on it
          var minStart: Long = Long.MaxValue
          var maxEnd: Long = Long.MinValue
          hss.getTaskStats.map { entry: (String, HadoopSliceStats) =>
            val start: Long = entry._2.getStartTime
            if (start < minStart) minStart = start
            val end: Long = entry._2.getFinishTime
            if (end > maxEnd) maxEnd = end
          }
          if (maxEnd - minStart < 1) hss.getCurrentDuration else maxEnd - minStart
        } catch {
          case _ => hss.getCurrentDuration
        }
      }

      case _ => {
        if (hss.getStatus.toString != "RUNNING")
          stepRunningTime.toLong * 1000L
        else
          hss.getCurrentDuration
      }
    }

    (diff / 1000L).toString
  }

  def getReduceProgress(hss: HadoopStepStats): String = {
    hss.getReduceProgress.isNaN match {
      case true => "100.0"
      case _    => "%3.2f" format (hss.getReduceProgress * 100.0)
    }
  }

  def getTuplesRead(hss: HadoopStepStats): String = {
    hss.getCounterValue(cascading.flow.StepCounters.Tuples_Read).toString
  }

  def getTuplesWritten(hss: HadoopStepStats): String = {
    hss.getCounterValue(cascading.flow.StepCounters.Tuples_Written).toString
  }

  def getFileBytesRead(hss: HadoopStepStats): String = {
    hss.getCounterValue("org.apache.hadoop.mapreduce.FileSystemCounter", "FILE_BYTES_READ").toString
  }

  def getFileBytesWritten(hss: HadoopStepStats): String = {
    hss.getCounterValue("org.apache.hadoop.mapreduce.FileSystemCounter", "FILE_BYTES_WRITTEN").toString
  }

  def getHdfsBytesRead(hss: HadoopStepStats): String = {
    hss.getCounterValue("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_READ").toString
  }

  def getHdfsBytesWritten(hss: HadoopStepStats): String = {
    hss.getCounterValue("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN").toString
  }

  def getMapperCount(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.JobCounter.TOTAL_LAUNCHED_MAPS).toString
  }

  def getReducerCount(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.JobCounter.TOTAL_LAUNCHED_REDUCES).toString
  }

  def getLocalMapperCount(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.JobCounter.DATA_LOCAL_MAPS).toString
  }

  def getRackMapperCount(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.JobCounter.RACK_LOCAL_MAPS).toString
  }

  def getCommittedHeapBytes(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.TaskCounter.COMMITTED_HEAP_BYTES).toString
  }

  def getGcTimeMillis(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.TaskCounter.GC_TIME_MILLIS).toString
  }

  def getCpuTimeMillis(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.TaskCounter.CPU_MILLISECONDS).toString
  }

  def getIoReadTimeMillis(hss: HadoopStepStats): String = {
    hss.getCounterValue(cascading.flow.SliceCounters.Read_Duration).toString
  }

  def getIoWriteTimeMillis(hss: HadoopStepStats): String = {
    hss.getCounterValue(cascading.flow.SliceCounters.Write_Duration).toString
  }

  def getFailedMapperCount(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.JobCounter.NUM_FAILED_MAPS).toString
  }

  def getFailedReducerCount(hss: HadoopStepStats): String = {
    hss.getCounterValue(org.apache.hadoop.mapreduce.JobCounter.NUM_FAILED_REDUCES).toString
  }
}
