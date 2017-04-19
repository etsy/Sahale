package com.etsy.sahale


import cascading.flow.{Flow, FlowStep}
import cascading.flow.hadoop.HadoopFlowStep
import cascading.stats.{CascadingStats, FlowStepStats}
import cascading.stats.hadoop.{HadoopStepStats, HadoopSliceStats}
import cascading.tap.Tap
import cascading.util.Util

import java.util.Properties

import spray.json._
import DefaultJsonProtocol._

import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.JavaConversions._


object StepStatus {
  val NO_JOB_ID = "NO_JOB_ID"
  val NOT_LAUNCHED = "NOT_LAUNCHED"

  val LOG: Logger = Logger.getLogger(classOf[StepStatus])
}

class StepStatus(val flow: Flow[_], val stepNumber: Int, val stepId: String, props: Properties) {
  import com.etsy.sahale.StepStatus._
  import com.etsy.sahale.JsonUtil

  private val state = mutable.Map[String, Any](
    "step_number"               -> stepNumber,
    "sources"                   -> Map.empty[String, Seq[String]],
    "sink"                      -> Map.empty[String, Seq[String]],
    "job_id"                    -> NO_JOB_ID,
    "step_id"                   -> stepId,
    "map_progress"              -> 0.0,
    "reduce_progress"           -> 0.0,
    "step_status"               -> NOT_LAUNCHED,
    "step_priority"             -> 5,
    "step_running_time"         -> 0L,
    "step_start_epoch_ms"       -> 0L,
    "step_submit_epoch_ms"      -> 0L,
    "step_end_epoch_ms"         -> 0L,
    "has_reduce_stage"          -> false,
    "counters"                  -> Map.empty[String, Map[String, Long]],
    "config_props"              -> Map.empty[String, String]
  )

  def send: JsValue = JsonUtil.toJsonMap(state.toMap).toJson

  def getStatus: String = stats.getStatus.toString

  // return reference to step status only if it was updated
  def update = state += (
    "has_reduce_stage"          -> stepHasReduceStage,
    "job_id"                    -> stats.getJobID,
    "step_running_time"         -> getStepRunningTime,
    "step_status"               -> getStatus,
    "map_progress"              -> getMapProgress,
    "reduce_progress"           -> getReduceProgress,
    "step_priority"             -> getStepPriority,
    "step_start_epoch_ms"       -> stats.getStartTime,
    "step_submit_epoch_ms"      -> stats.getSubmitTime,
    "step_end_epoch_ms"         -> stats.getFinishedTime,
    "counters"                  -> updateStepCounters,
    "config_props"              -> updateHadoopConf
  )

  // for unit tests
  override def equals(other: Any): Boolean = {
    if (other != null && other.isInstanceOf[StepStatus]) {
      val nss = other.asInstanceOf[StepStatus]
      this.state == nss.state
    } else {
      false
    }
  }

  // let caller pull from cached data when possible
  def get[T](key: String): T = state(key).asInstanceOf[T]

  // let caller pass in queries for the step encapsulated here
  def extract[T](fn: (HadoopFlowStep) => T): T = fn(step)

  def captureTaps(sources: Map[String, Seq[String]], sink: Map[String, Seq[String]]): Unit = state += (
    "sources"   -> sources,
    "sink"      -> sink
  )

  // will be reset for us by FlowTrackerStepStrategy
  private var stepStartMillis = System.currentTimeMillis

  def markStartTime: Unit = stepStartMillis = System.currentTimeMillis

  def getStepPriority: Int = step.getSubmitPriority

  def getStepRunningTime: Long = (System.currentTimeMillis - stepStartMillis) / 1000L

  def getMapProgress: Double = stats.getMapProgress.isNaN match {
    case true => 0.0
    case _    => JsonUtil.percent(stats.getMapProgress)
  }

  def getReduceProgress: Double = stats.getReduceProgress.isNaN match {
    case true => 0.0
    case _    => JsonUtil.percent(stats.getReduceProgress)
  }

  def stepHasReduceStage: Boolean = {
    step.getConfig.asInstanceOf[JobConf].getNumReduceTasks > 0L
  }

  def aggrFunc(group: String, key: String): Long = {
    stats.getCounterValue(group, key)
  }

  //////////// INTERNALS ////////////
  private lazy val step = flow.getFlowSteps.toList.filter {
    fs: FlowStep[_] => fs.getID == stepId
  }.head.asInstanceOf[HadoopFlowStep]

  private def stats = step.getFlowStepStats.asInstanceOf[HadoopStepStats]

  private def updateHadoopConf = propertiesToExtract.foldLeft(Map.empty[String, String]) {
    (acc, prop) => acc ++ Map(prop -> step.getConfig.asInstanceOf[JobConf].get(prop, ""))
  }

  private def updateStepCounters: Map[String, Map[String, Long]] = dumpCounters.groupBy[String] {
      i: (String, String, Long) => i._1
    }.map {
      case(k: String, v: Iterable[(String, String, Long)]) =>
        k -> v.map { vv => vv._2 -> vv._3 }.toMap
    }.toMap

  private def dumpCounters: Iterable[(String, String, Long)] = {
    for (g <- stats.getCounterGroups ; c <- stats.getCountersFor(g)) yield {
      (cleanGroupName(g), c, stats.getCounterValue(g, c))
    }
  }

  // if users want to track additional JobConf values, put the chosen keys in a CSV
  // list in flow-tracker.properties entry "sahale.step.selected.configs" at build time
  private val propertiesToExtract = Seq("sahale.additional.links", "scalding.step.descriptions") ++ {
    props.getProperty("sahale.step.selected.configs", "")
      .split(""",""").map { _.trim }.filter { _ != "" }.toSeq
  }

  private def cleanGroupName(name: String): String = {
    name match {
      case oah: String if (oah.startsWith("org.apache.hadoop.")) => cleanGroupName(oah.substring(18))
      case ic: String if (ic.indexOf("""$""") >= 0) => cleanGroupName(ic.substring(0, ic.indexOf("""$""")))
      case _ => name
    }
  }
}
