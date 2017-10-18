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

  val emptyProps = new java.util.Properties

  val expectedEdgeMap = Map[String, Set[Int]](
    "11" -> Set(33),
    "22" -> Set(44),
    "33" -> Set(44)
  )

  val expectedStepStatusMap = mutable.Map[String, StepStatus](
    "2AAA915A87DE4B52B2A56C249545C54D" -> new StepStatus(null, 11, "2AAA915A87DE4B52B2A56C249545C54D", emptyProps),
    "6EC78784266342CB9424E9875FF4299F" -> new StepStatus(null, 22, "6EC78784266342CB9424E9875FF4299F", emptyProps),
    "9FC4CA743ED5468EBC8C3CA76C6B12A6" -> new StepStatus(null, 33, "9FC4CA743ED5468EBC8C3CA76C6B12A6", emptyProps),
    "C4047D6DEBB6427B8B95DAF19D3E5DE2" -> new StepStatus(null, 44, "C4047D6DEBB6427B8B95DAF19D3E5DE2", emptyProps)
  )

  "A FlowGraphBuilder" should "extract a valid Edge Map from the planned Flow" in {
    val fgb = newFlowGraphBuilder
    fgb.extractVerticesAndEdgesFromDot(graphDotString)
    fgb.edgeMap should be (expectedEdgeMap)
  }

  "A FlowGraphBuilder" should "extract a valid StepStatus map from the planned Flow" in {
    val fgb = newFlowGraphBuilder
    fgb.extractVerticesAndEdgesFromDot(graphDotString)
    fgb.stepStatusMap should be (expectedStepStatusMap)
  }

  def newFlowGraphBuilder: FlowGraphBuilder = {
    new FlowGraphBuilder(null, mutable.Map[String, StepStatus](), mutable.Map[String, Set[Int]](), true)
  }

}

