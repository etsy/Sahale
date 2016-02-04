package com.etsy.sahale

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StepStatusSpec extends FlatSpec with ShouldMatchers {
  val stepNumber = 27
  val stepId = "F00DCAFE"

  "A StepStatus" should "instantiate steps in a valid state" in {
    val ss = new StepStatus(null, stepNumber, stepId, new java.util.Properties)

    ss.get[String]("step_id")                       should equal (stepId)
    ss.get[Int]("step_number")                      should equal (stepNumber)
    ss.get[Map[String, Seq[String]]]("sink")        should equal (Map.empty[String, Seq[String]])
    ss.get[Map[String, Seq[String]]]("sources")     should equal (Map.empty[String, Seq[String]])
    ss.get[String]("step_status")                   should equal (StepStatus.NOT_LAUNCHED)
    ss.get[Long]("step_running_time")               should equal (0L)
    ss.get[Double]("map_progress")                  should equal (0.0)
  }

}
