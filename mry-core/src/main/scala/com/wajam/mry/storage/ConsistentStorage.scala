package com.wajam.mry.storage

import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Consistent storage engine
 */
trait ConsistentStorage extends Storage {

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long)

}
