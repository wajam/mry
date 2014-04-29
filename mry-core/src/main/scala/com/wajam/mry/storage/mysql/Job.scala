package com.wajam.mry.storage.mysql

import com.wajam.nrv.utils.{TimestampIdGenerator, Startable}
import com.wajam.spnl._
import com.wajam.spnl.feeder.Feeder
import scala.util.Random
import com.wajam.commons.IdGenerator

/**
 * Marker trait allowing the definition of a Runnable Once job.
 * Should be mixed in to a Feeder and to the SPNL Task running it
 */
trait Job {
  def isProcessing: Boolean
  def currentJobId: String
  def start(jobId: String): Boolean
  def stop(): Boolean
}

object Job {
  val JobId = "job_id"
}

class TaskJob(feeder: Feeder with Job, action: TaskAction,
              persistence: TaskPersistence = NoTaskPersistence,
              context: TaskContext = new TaskContext,
              random: Random = Random)(implicit idGenerator: IdGenerator[Long] = new TimestampIdGenerator)
  extends Task(feeder, action, persistence,
           context, random)(idGenerator) with Job {
  def isProcessing: Boolean = feeder.isProcessing

  def currentJobId: String = feeder.currentJobId

  def start(jobId: String): Boolean = feeder.start(jobId)

  def stop(): Boolean = feeder.stop()
}