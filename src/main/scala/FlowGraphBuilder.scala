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
import cascading.util.Util

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Parses Cascading workflow plan
 *
 * @author Eli Reisman
 */
class FlowGraphBuilder(val flow: Flow[_], val stepStatusMap: mutable.Map[String, StepStatus]) {
  val ExtractVertexLabel = """\s+(\d+)\s+\[label\s+=\s+\"(\d+_[A-Fa-f0-9]+)\"\];""".r
  val ExtractVertexEdgeMapping = """\s+(\d+)\s+->\s+([0-9 \t]+);""".r
  val SOURCE_SIZE = 90
  val edgeMap = mutable.Map[String, Set[String]]()

  /**
   * Populates the data structures that manage the job DAG for this Flow.
   */
  def composeDag: Map[String, String] = {
    extractStepGraphFromFlow
    extractSourcesAndSinksFromStepGraph
    edgeMap.map { entry => (entry._1 -> entry._2.mkString("|")) }.toMap
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
    val tempMap = mutable.Map[String, String]()
    dotFile.split("\n").toList.map { line: String =>
      line match {
        case ExtractVertexLabel(node, label) => createVertex(tempMap, node, label)
        case ExtractVertexEdgeMapping(node, targetNodes) =>
          edgeMap(tempMap(node)) = targetNodes.trim.split("\\s+").map { tempMap(_) }.toSet
        case _ => // do nothing with non-matching lines
      }
    }
  }

  /**
   * Add a new vertex (MR job) to the map of the Flow
   */
  def createVertex(tempMap: mutable.Map[String, String], node: String, label: String): Unit = {
    val (stageNumber, stageId) = label.split("_") match {
      case Array(num: String, id: String) => (num, id)
      case _ => ("-1", "ERROR")
    }
    tempMap += (node -> stageNumber)
    stepStatusMap += (stageId -> new StepStatus(stageNumber, stageId, FlowTracker.props))
  }

  /**
   * Add sources and a sink to each FlowStep (job stage) we track.
   * Sinks, Sources, and Fields are tracked in StepStatus objects per
   * job stage, but are most easily extracted here in the FlowTracker.
   */
  def extractSourcesAndSinksFromStepGraph: Unit = {
    val sourcesToStageMap = mutable.Map[String, List[String]]()
    val sinkToStageMap = mutable.Map[String, String]()

    flow.getFlowSteps.toList.map { fs: FlowStep[_] =>
      val stepNum = fs.getStepNum.toString

      // capture sink and sink fields data
      val sink = sanitizePathName(fs.getSink.getIdentifier)
      val sinkFields = fs.getSink.getSinkFields.toString.replace(" ", "").replace("'", "")
      sinkToStageMap(sink) = stepNum

      // capture (multiple) sources and source fields data
      val sourcesFields = fs.getSources.toSet.map { tap: Tap[_, _, _] =>
        tap.getSourceFields.toString.replace(" ", "").replace("'", "")
      }.mkString(";")

      val sources = fs.getSources.toSet
        .map { tap: Tap[_, _, _] =>
          val sourceId = sanitizePathName(tap.getIdentifier)
          sourcesToStageMap(sourceId) = sourcesToStageMap.getOrElse(sourceId, null) match {
            case sources: List[String] => sources ++ List(stepNum)
            case _ => List(stepNum)
          }
          sourceId
        }.mkString(",")

      // these are updated only once per workflow step, here before the job runs
      val conf = fs.getConfig.asInstanceOf[JobConf]
      stepStatusMap(fs.getID).setConfigurationProperties(conf)
      stepStatusMap(fs.getID).setSourcesAndSink(sources, sourcesFields, sink, sinkFields)
    }

    // finish up adding extra sink->source edges to our job graph
    sinkToStageMap.map { sinkEntry =>
      sourcesToStageMap.getOrElse(sinkEntry._1, null) match {
        case sourceList: List[String] => sourceList.map {
          source => updateEdgeMap(sinkEntry._2, source)
        }
        case _ =>
      }
    }
  }

  def updateEdgeMap(key: String, value: String): Unit = {
    edgeMap.getOrElse(key, null) match {
      case previousEntries: Set[String] => edgeMap(key) = previousEntries ++ Set(value)
      case _ => edgeMap(key) = Set(value)
    }
  }

  def sanitizePathName(path: String): String = {
    // TODO: handle "cascading.tmp.dir" also, Hadoop tmp can be overriden
    val hadoopTmpDir = flow.asInstanceOf[HadoopFlow].getConfig.get("hadoop.tmp.dir", "/tmp/cache");
    val PipePath = ("(" + hadoopTmpDir + """[A-Za-z0-9._/-]+)_[A-Fa-f0-9]+""").r
    path match {
      case PipePath(shortName) => shortName
      case namedPipe: String => cleanPipeName(namedPipe)
      case _ => FlowTracker.UNKNOWN
    }
  }

  def cleanPipeName(str: String): String = {
    val (clean, junk) = str.partition { ch: Char =>
      ch match {
        case ch: Char if ch.isLetterOrDigit || ch == '_' || ch == '/' || ch == '-' || ch == '.' => true
        case _ => false
      }
    }
    clean match {
      case str: String if str.length >= SOURCE_SIZE => str.substring(0, SOURCE_SIZE) + "..."
      case _ => clean
    }
  }
}
