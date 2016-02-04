package com.etsy.sahale


import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Logger

import org.jgrapht.ext.{IntegerNameProvider, VertexNameProvider}

import cascading.flow.{Flow, FlowStep}
import cascading.flow.hadoop.HadoopFlow
import cascading.flow.planner.BaseFlowStep
import cascading.stats.{CascadingStats, FlowStepStats}
import cascading.stats.hadoop.HadoopStepStats
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.util.Util

import scala.collection.mutable
import scala.collection.JavaConversions._


object FlowGraphBuilder {
  val LOG = Logger.getLogger(classOf[FlowGraphBuilder])
}

/**
 * Parses Cascading workflow plan
 *
 * @author Eli Reisman
 */
class FlowGraphBuilder(flow: Flow[_],
  val stepStatusMap: mutable.Map[String, StepStatus],
  val edgeMap: mutable.Map[String, Set[Int]], isTest: Boolean = false) {
  import com.etsy.sahale.FlowGraphBuilder.LOG

  val ExtractVertexLabel = """\s+(\d+)\s+\[label\s+=\s+\"(\d+_[A-Fa-f0-9]+)\"\];""".r
  val ExtractVertexEdgeMapping = """\s+(\d+)\s+->\s+([0-9 \t]+);""".r

  /**
   * Populates the data structures that manage the job DAG for this Flow.
   */
  if (!isTest) {
    extractStepGraphFromFlow
    extractSourcesAndSinksFromStepGraph
  }

  /**
   * Parse DOT-format graph output from Flow into a Scala Map.
   */
  def extractStepGraphFromFlow: Unit = {
    val writer = new java.io.StringWriter
    Util.writeDOT(
      writer,
      cascading.flow.Flows.getStepGraphFrom(flow),
      new IntegerNameProvider[BaseFlowStep[_]],
      new VertexNameProvider[FlowStep[_]] {
        override def getVertexName(flowStep: FlowStep[_]): String = {
          flowStep.getStepNum + "_" + flowStep.getID
        }
      },
      null // this is the EdgeNameProvider, we don't use it
    )

    // parse the file
    extractVerticesAndEdgesFromDot(writer.toString)
  }

  // two step process to extract edge and vertex data from DOT file
  def extractVerticesAndEdgesFromDot(dotFile: String): Unit = {
    dotFile.split("\n").foldLeft(Map.empty[String, Int]) {
      (tempMap: Map[String, Int], line: String) =>
        line match {
          case ExtractVertexLabel(node, label) => tempMap ++ createVertex(node, label)

          case ExtractVertexEdgeMapping(node, targetNodes) =>
            edgeMap(tempMap(node).toString) = targetNodes.trim.split("\\s+").map { tempMap(_) }.toSet
            tempMap

          case _ => tempMap // do nothing with non-matching lines
        }
    }
  }

  /**
   * Add a new vertex (MR job) to the map of the Flow
   */
  def createVertex(node: String, label: String): Map[String, Int] = {
    val (stageNumber, stageId) = label.split("_") match {
      case Array(num: String, id: String) => (num.toInt, id)
      case _ => (-1, "ERROR")
    }
    stepStatusMap += (stageId -> new StepStatus(flow, stageNumber, stageId, FlowTracker.props))
    Map(node -> stageNumber)
  }

  /**
   * Add sources and a sink to each FlowStep (job stage) we track.
   * Sinks, Sources, and Fields are tracked in StepStatus objects per
   * job stage, but are most easily extracted here where the graph
   * is initialized. Called from FlowTracker.
   */
  def extractSourcesAndSinksFromStepGraph: Unit = {
    val sourcesToStageMap = mutable.Map[String, Seq[Int]]()
    val sinkToStageMap = mutable.Map[String, Int]()

    flow.getFlowSteps.toList.map { fs: FlowStep[_] =>
      val stepNum = fs.getStepNum

      // capture sink and sink fields data
      val sink = sanitizePathName(fs.getSink.getIdentifier)
      val sinkFields = extractFields(fs.getSink.getSinkFields)
      sinkToStageMap(sink) = stepNum

      // capture (multiple) sources and source fields data
      val sources = fs.getSources.toSet.map { tap: Tap[_, _, _] =>
        val sourceId = sanitizePathName(tap.getIdentifier)
        val sourceFields = extractFields(tap.getSourceFields)
        sourcesToStageMap(sourceId) = sourcesToStageMap.getOrElse(sourceId, Seq.empty[Int]) ++ Seq[Int](stepNum)
        (sourceId -> sourceFields)
      }.toMap[String, Seq[String]]

      // these are updated only once per workflow step, here before the job runs
      stepStatusMap(fs.getID).captureTaps(sources, Map[String, Seq[String]](sink -> sinkFields))
    }

    // finish up adding extra sink->source edges to our job graph
    sinkToStageMap.map { sinkEntry =>
      sourcesToStageMap.get(sinkEntry._1) match {
        case Some(srcs) =>
          srcs.asInstanceOf[Seq[Int]].map { updateEdgeMap(sinkEntry._2.toString, _) }
        case None       =>
      }
    }
  }

  def updateEdgeMap(key: String, value: Int): Unit = {
    edgeMap(key) = edgeMap.getOrElse(key, Set.empty[Int]) ++ Set(value)
  }

  def sanitizePathName(path: String): String = path match {
    case p: String if p != null => p
    case _                      => FlowTracker.UNKNOWN
  }

  def extractFields(f: Fields): Seq[String] = (0 until f.size).map {
    ndx: Int => f.get(ndx).asInstanceOf[Any].toString.trim.replaceAll("'","")
  }.toSeq
}
