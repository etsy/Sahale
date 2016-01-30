package com.etsy.sahale

import java.util.List

import cascading.flow.{FlowStep, Flow, FlowStepStrategy}

// same approach as in Scalding
object FlowStepStrategies {
  /**
   * Returns a new FlowStepStrategy that runs both strategies in sequence.
   * If either of the strategies is null it is skipped
   */
  def plus[A](l: FlowStepStrategy[A], r: FlowStepStrategy[A]): FlowStepStrategy[A] =
    new FlowStepStrategy[A] {
      override def apply(
                          flow: Flow[A],
                          predecessorSteps: List[FlowStep[A]],
                          flowStep: FlowStep[A]): Unit = {
        if (l != null) {
          l.apply(flow, predecessorSteps, flowStep)
        }

        if (r != null) {
          r.apply(flow, predecessorSteps, flowStep)
        }
      }
    }
}
