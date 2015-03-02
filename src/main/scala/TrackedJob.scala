package com.etsy.sahale


import cascading.cascade.{Cascade, CascadeConnector}
import cascading.flow.Flow

import java.util.concurrent.atomic.AtomicBoolean

import com.twitter.scalding._


/**
 * Your jobs should inherit from this class to inject job tracking functionality.
 *
 * @author Eli Reisman
 */
class TrackedJob(args: Args) extends com.twitter.scalding.Job(args) {
  @transient private val done = new AtomicBoolean(false)

  /**
   * Override run() to control the lifecycle of the tracker threads using try/finally.
   * This will be run on the management process, your own Job code will run on the cluster.
   *
   * ***WARNING***
   * Scalding 0.12.0 breaks compatibiilty here: for Scalding 0.8.5, this signature must be:
   * "override def run(implicit mode: Mode) = { ..."
   */
  override def run: Boolean = {
    mode match {
      // only track Hadoop cluster jobs marked "--track-job"
      case Hdfs(_, _) => if (args.boolean("track-job")) runTrackedJob else super.run
      case _ => super.run
    }
  }

  def runTrackedJob(implicit mode: Mode) = {
    try {
      val flow = buildFlow
      trackThisFlow(flow)
      flow.complete
      flow.getFlowStats.isSuccessful // return Boolean
    } catch {
      case t: Throwable => throw t
    } finally {
      // ensure all threads are cleaned up before we propagate exceptions or complete the run.
      done.set(true)
      Thread.sleep(100)
    }
  }

  private def trackThisFlow(f: Flow[_]): Unit = { (new Thread(new FlowTracker(f, done))).start }
}
