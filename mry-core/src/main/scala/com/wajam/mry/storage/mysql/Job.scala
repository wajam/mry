package com.wajam.mry.storage.mysql

import com.wajam.nrv.utils.Startable

/**
 * Marker trait allowing the definition of a Runnable Once job.
 * Should be mixed in to a Feeder and to the SPNL Task running it
 */
trait Job extends Startable {
  def getCurrentId: String
}
