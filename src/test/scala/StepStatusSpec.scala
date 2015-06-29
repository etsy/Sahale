package com.etsy.sahale

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StepStatusSpec extends FlatSpec with ShouldMatchers {

  val expected = Map[String, Any](
    "stepnumber"                -> "foo",
    "sources"                   -> FlowTracker.UNKNOWN,
    "sink"                      -> FlowTracker.UNKNOWN,
    "sourcesfields"             -> FlowTracker.UNKNOWN,
    "sinkfields"                -> FlowTracker.UNKNOWN,
    "jobid"                     -> "NO_JOB_ID",
    "stepid"                    -> "bar",
    "mapprogress"               -> "0.00",
    "reduceprogress"            -> "0.00",
    "stepstatus"                -> "NOT_LAUNCHED",
    "steprunningtime"           -> "0",
    "steppriority"              -> "0",
    "step_start_epoch_ms"       -> "0",
    "step_submit_epoch_ms"      -> "0",
    "step_end_epoch_ms"         -> "0",
    "counters"                  -> Map[String, Any](),
    "configuration_properties"  -> Map[String, String]()
  )

  "A StepStatus" should "return a valid initial map given fixed input arguments" in {
    val that = (new StepStatus("foo", "bar", new java.util.Properties))

    that.stepNumber should equal (expected("stepnumber"))
    that.sources should equal (expected("sources"))
    that.sink should equal (expected("sink"))
    that.sourcesFields should equal (expected("sourcesfields"))
    that.sinkFields should equal (expected("sinkfields"))
    that.jobId should equal (expected("jobid"))
    that.stepId should equal (expected("stepid"))
    that.mapProgress should equal (expected("mapprogress"))
    that.reduceProgress should equal (expected("reduceprogress"))
    that.stepStatus should equal (expected("stepstatus"))
    that.stepRunningTime.toString should equal (expected("steprunningtime"))
    that.stepPriority should equal (expected("steppriority"))
    that.stepStartEpochMs should equal (expected("step_start_epoch_ms"))
    that.stepSubmitEpochMs should equal (expected("step_submit_epoch_ms"))
    that.stepEndEpochMs should equal (expected("step_end_epoch_ms"))
    that.counters should equal (expected("counters"))
    that.configurationProperties should equal (expected("configuration_properties"))
  }

}

