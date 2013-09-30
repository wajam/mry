package com.wajam.mry.storage

import com.wajam.commons.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import com.wajam.mry.execution.{Transaction, Value}
import com.wajam.commons.Closable

/**
 * Consistent storage engine
 */
trait ConsistentStorage extends Storage {

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp]

  /**
   * Setup the function which returns the most recent timestamp considered as consistent by the Consistency manager
   * for the specified token range. The consistency of the records more recent that the consistent timestamp is
   * unconfirmed and these records must be excluded from processing tasks such as GC or percolation.
   */
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp)

  /**
   * Returns the mutation transactions from and up to the given timestamps inclusively for the specified token ranges.
   */
  def readTransactions(from: Timestamp, to: Timestamp, ranges: Seq[TokenRange]): Iterator[TransactionRecord] with Closable

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
