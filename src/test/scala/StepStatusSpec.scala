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
    val that = (new StepStatus("foo", "bar", new java.util.Properties)).toMap
    that.map { e => e._2 should be (expected(e._1)) }
  }

}

