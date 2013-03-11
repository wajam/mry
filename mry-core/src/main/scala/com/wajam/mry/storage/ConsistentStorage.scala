package com.wajam.mry.storage

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import com.wajam.mry.execution.{Transaction, Value}

/**
 * Consistent storage engine
 */
trait ConsistentStorage extends Storage {

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp]

  /**
   * Returns the mutation transactions from and up to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(from: Timestamp, to: Timestamp, ranges: Seq[TokenRange]): Iterator[TransactionRecord]

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long)

}

trait TransactionRecord {
  def token: Long

  def timestamp: Timestamp

  def applyTo(transaction: Transaction)
}
