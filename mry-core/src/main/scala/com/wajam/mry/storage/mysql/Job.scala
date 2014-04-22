package com.wajam.mry.storage.mysql

import com.wajam.nrv.utils.Startable

/**
 * Marker trait allowing the definition of a Runnable Once job.
 * Should be mixed in to a Feeder and to the SPNL Task running it
 */
trait Job {
  def isStarted: Boolean
  def currentJobId: String
  def start(jobId: String): Boolean
  def stop(): Boolean
}
