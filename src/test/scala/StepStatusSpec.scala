package com.etsy.sahale

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StepStatusSpec extends FlatSpec with ShouldMatchers {

  val expected = Map[String, String](
    "stepnumber" -> "foo",
    "sources" -> FlowTracker.UNKNOWN,
    "sink" -> FlowTracker.UNKNOWN,
    "sourcesfields" -> FlowTracker.UNKNOWN,
    "sinkfields" -> FlowTracker.UNKNOWN,
    "jobid" -> "NO_JOB_ID",
    "stepid" -> "bar",
    "mapprogress" -> "0.00",
    "reduceprogress" -> "0.00",
    "stepstatus" -> "NOT_LAUNCHED",
    "steprunningtime" -> "0",
    "maptasks" -> "0",
    "reducetasks" -> "0",
    "hdfsbytesread" -> "0",
    "hdfsbyteswritten" -> "0",
    "filebytesread" -> "0",
    "filebyteswritten" -> "0",
    "tuplesread" -> "0",
    "tupleswritten" -> "0",
    "datalocalmaptasks" -> "0",
    "racklocalmaptasks" -> "0",
    "committedheapbytes" -> "0",
    "gcmillis" -> "0",
    "cpumillis" -> "0",
    "ioreadmillis" -> "0",
    "iowritemillis" -> "0",
    "failedmaptasks" -> "0",
    "failedreducetasks" -> "0"
  )

  "A StepStatus" should "return a valid initial map given fixed input arguments" in {
    val that = (new StepStatus("foo", "bar")).toMap
    that.map { e => e._2 should be (expected(e._1)) }
  }

}

