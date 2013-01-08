package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Fetches all current defined (not null) data on a table.
 * When it finished, it loops over and starts again with the oldest current data.
 */
class TableContinuousFeeder(storage: MysqlStorage, table: Table, rowsToFetch: Int = 1000)
  extends CachedDataFeeder with Logging {

  var context: TaskContext = null
  var lastRecord: Option[Record] = None

  def init(context: TaskContext) {
    //TODO add elements in context to make sure tests work
    this.context = context
  }

  def loadMore() = {

    var transaction: MysqlTransaction = null
    try {
      val fromRecord = lastRecord
      transaction = storage.createStorageTransaction
      val recordsIterator = transaction.getAllLatest(table, rowsToFetch, lastRecord)

      var records = Iterator.continually({
        if (recordsIterator.next()) {
          lastRecord = Some(recordsIterator.record)
          lastRecord
        } else {
          None
        }
      }).takeWhile(_.isDefined).flatten

      // since getAllLatest return the "from" record, we remove it first
      records = fromRecord match {
        case Some(record) => records.filterNot(_ == record)
        case None => records
      }

      // Restart if tree is still empty
      if (records.isEmpty) {
        lastRecord = None
      }

      records.map(record => Map(
        "keys" -> record.accessPath.keys,
        "token" -> record.token.toString,
        "value" -> record.value
      )).toList
    } catch {
      case e: Exception => {
        log.error("An exception occured while loading more elements from table {}", table.depthName("_"), e)
        Nil
      }
    } finally {
      if (transaction != null) {
        transaction.commit()
      }
    }
  }

  def ack(data: Map[String, Any]) {}

  def kill() {}
}
