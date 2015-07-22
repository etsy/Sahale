package com.etsy.sahale

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FlowStatusSpec extends FlatSpec with ShouldMatchers {

  val initialExpected = Map[String, String](
    "jt_url" -> FlowTracker.UNKNOWN,
    "user_name" -> FlowTracker.UNKNOWN,
    "flow_status" -> "NOT_LAUNCHED",
    "total_stages" -> "0",
    "flow_progress" -> "0.00",
    "flow_duration" -> "0",
    "flow_hdfs_bytes_written" -> "0",
    "flow_priority" -> "0",
    "cascade_id" -> FlowTracker.UNKNOWN,
    "yarn_job_history" -> FlowTracker.NOT_YARN_JOB,
    "hdfs_working_dir" -> FlowTracker.UNKNOWN,
    "flow_start_epoch_ms"       -> "0",
    "flow_submit_epoch_ms"      -> "0",
    "flow_end_epoch_ms"         -> "0"
  )

  "A FlowStatus" should "provide a default initial state before the job launches" in {
    FlowStatus.initial should be (initialExpected)
  }

  "A FlowStatus" should "require a valid Flow instance" in {
    val fs = new FlowStatus(null)
    intercept[NullPointerException] {
      fs.toMap
    }
  }

}

