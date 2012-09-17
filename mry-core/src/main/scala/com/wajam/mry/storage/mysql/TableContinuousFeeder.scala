package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.{TaskContext, Feeder}
import collection.mutable
import com.wajam.mry.execution.Timestamp

/**
 *
 */
class TableContinuousFeeder(storage: MysqlStorage, table: Table) extends Feeder with Logging {
  var context: TaskContext = null

  var nextToken: Long = 0
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
      Some(cache.dequeue()) // TODO
    }
  }

  def loadMore() {
    debug("WE HAVE TO GO DEEPER!") // TODO write a decent debug message

    var transaction: MysqlTransaction = null
    try {
      transaction = storage.getStorageTransaction
      val mutations = transaction.getTimeline(table, nextTimestamp, 1000)

      for (mutation <- mutations) {
        // Find next timestamp
        for (timestamp <- mutation.oldTimestamp if timestamp > nextTimestamp) nextTimestamp = timestamp.value + 1L

        for (value <- mutation.newValue) cache.enqueue(Map("keys" -> mutation.accessPath.keys, "value" -> value))
      }
    } finally {
      if (transaction != null) {
        transaction.commit()
      }
    }
  }

  def kill() {}
}
