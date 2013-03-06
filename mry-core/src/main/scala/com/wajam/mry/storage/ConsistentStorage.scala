package com.wajam.mry.storage

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange

/**
 * Consistent storage engine
 */
trait ConsistentStorage extends Storage {

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp]

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long)

}
