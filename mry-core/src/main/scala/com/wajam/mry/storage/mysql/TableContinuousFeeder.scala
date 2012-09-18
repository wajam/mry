package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.{TaskContext, Feeder}
import collection.mutable
import com.wajam.mry.execution.Timestamp

object TableContinuousFeeder {
  val ROWS_TO_FETCH = 1000
}

/**
 *
 */
class TableContinuousFeeder(storage: MysqlStorage, table: Table, rowsToFetch: Int = TableContinuousFeeder.ROWS_TO_FETCH)
  extends Feeder with Logging {

  var context: TaskContext = null
  var nextTimestamp: Timestamp = 0

  val cache = new mutable.Queue[Map[String, Any]]()

  def init(context: TaskContext) {
    this.context = context
  }

  def next(): Option[Map[String, Any]] = {
    if (cache.isEmpty) {
      loadMore()
      None
    } else {
      Some(cache.dequeue())
    }
  }

  private def loadMore() {

    var transaction: MysqlTransaction = null
    try {
      transaction = storage.getStorageTransaction

      val values = transaction.getAllLatest(table, nextTimestamp, Timestamp.now, rowsToFetch)

      while (values.next()) {
        val record = values.record
        cache.enqueue(Map("keys" -> record.accessPath.keys, "value" -> record.value))
      }

      // Restart if tree is still empty
      if (cache.isEmpty) {
        nextTimestamp = 0
      }

    } finally {
      if (transaction != null) {
        transaction.commit()
      }
    }
  }

  def kill() {}
}
