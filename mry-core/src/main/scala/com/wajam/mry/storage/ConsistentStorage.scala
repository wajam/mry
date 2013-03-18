package com.wajam.mry.storage

import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.TokenRange
import com.wajam.mry.execution.{Transaction, Value}
import com.wajam.nrv.utils.Closable

/**
 * Consistent storage engine
 */
trait ConsistentStorage extends Storage {

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp]

  /**
   * Set the most recent timestamp considered as consistent by the Consistency manager for the specified token ranges.
   * The consistency of the records more recent than that timestamp is unconfirmed and must be excluded from the storage
   * job processing (e.g. GC or percolation)
   */
  def setLastConsistentTimestamp(timestamp: Timestamp, ranges: Seq[TokenRange])

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
