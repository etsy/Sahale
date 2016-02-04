package com.etsy.sahale

import java.util.Properties

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FlowStatusSpec extends FlatSpec with ShouldMatchers {

  "A FlowStatus" should "provide a default initial state before the job launches" in {
    val fs = getTestFlowStatus
    fs.get[String]("flow_name")     should equal (FlowStatus.TEST_FLOW)
    fs.get[String]("jt_url")        should equal (FlowTracker.UNKNOWN)
    fs.get[Int]("total_stages")     should equal (0)
    fs.get[Map[String, Any]]("aggregated") should equal (Map.empty[String, Any])
    fs.get[String]("flow_id")       should equal (FlowStatus.TEST_ID)
    fs.get[Long]("flow_duration")   should equal (0L)
  }

  "A FlowStatus" should "require a valid Flow instance in production use" in {
    intercept[NullPointerException] {
      new FlowStatus(null, new Properties())
    }
  }

  "A FlowStatus" should "allow registration of aggregator functions" in {
    val fs = getTestFlowStatus
    val func: () => Any = () => { (1 to 5).reduce(_+_) }
    fs.registerAggregators( Map("test" -> func) )
  }

  def getTestFlowStatus: FlowStatus = new FlowStatus(null, new Properties(), Array.empty[String], true)
}
