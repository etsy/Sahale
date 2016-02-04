package com.etsy.sahale


import cascading.flow.{FlowStep, Flow, FlowStepStrategy}
import scala.collection.mutable

class FlowTrackerStepStrategy[A](val stepMap: mutable.Map[String, StepStatus]) extends FlowStepStrategy[A] {

  override def apply(flow: Flow[A], predecessors: java.util.List[FlowStep[A]], current: FlowStep[A]) = {
    stepMap.get(current.getID).map { step => step.markStartTime }
  }

}
