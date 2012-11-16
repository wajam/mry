package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext
import com.wajam.scn.Timestamp
import com.wajam.scn.storage.TimestampUtil

/**
 * Fetches all current defined (not null) data on a table.
 * When it finished, it loops over and starts again with the oldest current data.
 */
class TableContinuousFeeder(storage: MysqlStorage, table: Table, rowsToFetch: Int = 1000)
  extends CachedDataFeeder with Logging {

  var context: TaskContext = null
  var nextTimestamp: Timestamp = TimestampUtil.MIN


  def init(context: TaskContext) {
    //TODO add elements in context to make sure tests work
    this.context = context
  }

  def loadMore() = {

    var transaction: MysqlTransaction = null
    try {
      transaction = storage.createStorageTransaction

      val values = transaction.getAllLatest(table, nextTimestamp, TimestampUtil.now, rowsToFetch)

      val records = Iterator.continually({
        if (values.next()) {
          Some(values.record)
        } else {
          None
        }
      }).takeWhile(_.isDefined).flatten

      // Restart if tree is still empty
      if (records.isEmpty) {
        nextTimestamp = TimestampUtil.MIN
      }

      records.map(record => Map("keys" -> record.accessPath.keys, "value" -> record.value)).toIterable
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

  def kill() {}
}
