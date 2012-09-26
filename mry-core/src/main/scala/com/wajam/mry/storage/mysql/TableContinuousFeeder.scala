package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.{TaskContext, Feeder}
import collection.mutable
import com.wajam.mry.execution.Timestamp

/**
 * Fetches all current defined (not null) data on a table.
 * When it finished, it loops over and starts again with the oldest current data.
 */
class TableContinuousFeeder(storage: MysqlStorage, table: Table, rowsToFetch: Int = 1000)
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
      transaction = storage.createStorageTransaction

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
