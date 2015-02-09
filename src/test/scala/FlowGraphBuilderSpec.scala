package com.etsy.sahale


import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable


@RunWith(classOf[JUnitRunner])
class FlowGraphBuilderSpec extends FlatSpec with ShouldMatchers {

  val graphDotString = {
    scala.io.Source.fromInputStream(
      Thread.currentThread.getContextClassLoader.getResourceAsStream(
        "test-graph.txt"
      ),
      "UTF-8"
    ).getLines().mkString("\n")
  }

  val expectedEdgeMap = Map[String, Set[String]](
    "11" -> Set("33"),
    "22" -> Set("44"),
    "33" -> Set("44")
  )

  val expectedStepStatusMap = mutable.Map[String, StepStatus](
    "2AAA915A87DE4B52B2A56C249545C54D" -> new StepStatus("11", "2AAA915A87DE4B52B2A56C249545C54D"),
    "6EC78784266342CB9424E9875FF4299F" -> new StepStatus("22", "6EC78784266342CB9424E9875FF4299F"),
    "9FC4CA743ED5468EBC8C3CA76C6B12A6" -> new StepStatus("33", "9FC4CA743ED5468EBC8C3CA76C6B12A6"),
    "C4047D6DEBB6427B8B95DAF19D3E5DE2" -> new StepStatus("44", "C4047D6DEBB6427B8B95DAF19D3E5DE2")
  )

  "A FlowGraphBuilder" should "extract a valid Edge Map from the planned Flow" in {
    val fgb = new FlowGraphBuilder(null, mutable.Map[String, StepStatus]())
    fgb.extractVerticesAndEdgesFromDot(graphDotString)
    fgb.edgeMap.toMap should be (expectedEdgeMap)
  }

  "A FlowGraphBuilder" should "extract a valid StepStatus map from the planned Flow" in {
    val fgb = new FlowGraphBuilder(null, mutable.Map[String, StepStatus]())
    fgb.extractVerticesAndEdgesFromDot(graphDotString)
    prep(fgb.stepStatusMap) should be (prep(expectedStepStatusMap))
  }

  def prep(map: mutable.Map[String, StepStatus]): String = {
    map.keys.toList.sortWith(_ < _).map { k => k + "\t" + map(k) }.mkString("\n")
  }
}

