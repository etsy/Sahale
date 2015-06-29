package com.etsy.sahale


import cascading.flow._
import org.apache.log4j.Logger
import org.apache.hadoop.mapred.JobConf
import scala.collection.JavaConverters._

object FlowTrackerStepStrategy {
  val LOG = Logger.getLogger(classOf[FlowTrackerStepStrategy])
}

class FlowTrackerStepStrategy(val stepMap: scala.collection.mutable.Map[String, StepStatus]) extends FlowStepStrategy[JobConf] {
  import com.etsy.sahale.FlowTrackerStepStrategy._

  override def apply(flow: Flow[JobConf], predecessors: java.util.List[FlowStep[JobConf]], current: FlowStep[JobConf]) = {
    stepMap.get(current.getID).map { step =>
      step.markStartTime
      LOG.info(
        "FlowTrackerStepStrategy marking Step " + current.getStepNum + " with Step ID " +
        current.getID + " started at epoch_ms: " +  step.stepRunningTime
      )
    }
  }

}
