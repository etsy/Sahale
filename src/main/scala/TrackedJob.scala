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

  /**
   * Override run() to control the lifecycle of the tracker threads using try/finally.
   * This will be run on the management process, your own Job code will run on the cluster.
   *
   * Older Scalding versions break compatibiilty here. On older Scalding, this signature should be:
   * override def run(implicit mode: Mode)
   */
  override def run: Boolean = {
    mode match {
      // only track Hadoop cluster jobs marked "--track-job"
      case Hdfs(_, _) => if (args.boolean("track-job")) runTrackedJob else super.run
      case _ => super.run
    }
  }

  def runTrackedJob(implicit mode: Mode) = {
    var done: AtomicBoolean = null
    var thread: Thread = null

    try {
      val flow = buildFlow
      val serverHostPort: String = args.optional("server").getOrElse("")
      val disableProgressBar = args.boolean("disable-progress-bar")
      done = new AtomicBoolean(false) // replace the flag for iterative runs with multiple Flows
      thread = new Thread(new FlowTracker(flow, done, serverHostPort, disableProgressBar))
      thread.start
      flow.complete
      flow.getFlowStats.isSuccessful // return Boolean
    } catch {
      case t: Throwable => throw t
    } finally {
      // ensure the thread is cleaned up when the job is complete
      if (null != done) {
        done.set(true)
        if (null != thread) {
          try { thread.join(); } catch { case ignored: InterruptedException => }
        }
      }
    }
  }

}
