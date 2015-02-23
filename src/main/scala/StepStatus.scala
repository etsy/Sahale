package com.etsy.sahale


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
  var counters = Map[String, Any]()
  var hdfsBytesWritten = 0L

/*
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
*/

  override def toString: String = {
    toMap.toString
  }

  def toMap: Map[String, Any] = {
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
      "counters" -> counters
/*
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
      "failedreducetasks" -> failedReduceTasks,
*/
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
  def update(hadoopStepStats: HadoopStepStats): Unit = {
    jobId = hadoopStepStats.getJobID
    mapProgress = getMapProgress(hadoopStepStats)
    reduceProgress = getReduceProgress(hadoopStepStats)
    stepRunningTime = getStepRunningTime(hadoopStepStats) // checks old stepStatus value - MUST be updated first!
    stepStatus = hadoopStepStats.getStatus.toString 
    updateStepCounters(hadoopStepStats)
  }

  // Calling captureDetail every update just for this is not worth it.
  // Just be sure final update of run picks up accurate stats for this.
  def getStepRunningTime(hss: HadoopStepStats): String = {
    val forceUpdate: Boolean = (stepStatus == "RUNNING" && hss.getStatus.toString != "RUNNING")
    val diff: Long = forceUpdate match {
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
          if (maxEnd - minStart < 1L) 0L else (maxEnd - minStart) / 1000L
        } catch {
          case _ => 0L
        }
      }
      case _ => {
        stepRunningTime.toLong
      }
    }
    diff.toString
  }

  def updateStepCounters(hss: HadoopStepStats): Unit = {
    counters = dumpCounters(hss).foldLeft(mutable.Map[String, Map[String, Long]]()) {
      (acc, next) => acc.getOrElse(next._1, None) match {
        case None => acc(next._1) = Map(next._2 -> next._3) ; acc
        case _    => acc(next._1) = Map(next._2 -> next._3) ++ acc(next._1) ; acc
      }
    }.toMap
  }

  def dumpCounters(hss: HadoopStepStats): Iterable[(String, String, Long)] = {
    for (g <- hss.getCounterGroups ; c <- hss.getCountersFor(g)) yield {
      (cleanGroupName(g), c, hss.getCounterValue(g, c))
    }
  }

  def cleanGroupName(name: String): String = {
    name match {
      case oah: String if (oah.startsWith("org.apache.hadoop.")) => cleanGroupName(oah.substring(18))
      case ic: String if (ic.indexOf("""$""") >= 0) => cleanGroupName(ic.substring(0, ic.indexOf("""$""")))
      case _ => name
    }
  }

  def getMapProgress(hss: HadoopStepStats): String = {
    hss.getMapProgress.isNaN match {
      case true => "0.00"
      case _    => "%3.2f" format (hss.getMapProgress * 100.0)
    }
  }

  def getReduceProgress(hss: HadoopStepStats): String = {
    hss.getReduceProgress.isNaN match {
      case true => "0.00"
      case _    => "%3.2f" format (hss.getReduceProgress * 100.0)
    }
  }

  def getHdfsBytesWritten(hss: HadoopStepStats): Long = {
    hss.getCounterValue("org.apache.hadoop.mapreduce.FileSystemCounter", "HDFS_BYTES_WRITTEN")
  }
}
