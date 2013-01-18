package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.spnl.TaskContext
import com.wajam.nrv.service.TokenRange

/**
 * Fetches all current defined (not null) data on a table.
 * When it finished, it loops over and starts again with the oldest current data.
 */
class TableContinuousFeeder(storage: MysqlStorage, table: Table, tokenRanges: Seq[TokenRange], rowsToFetch: Int = 1000)
  extends CachedDataFeeder with Logging {

  import TableContinuousFeeder._

  var context: TaskContext = null
  var lastRecord: Option[Record] = None
  var currentRange: Option[TokenRange] = None

  def init(context: TaskContext) {
    this.context = context

    val data = context.data
    lastRecord = if (data.contains(Keys) && data.contains(Token) && data.contains(Timestamp))
    {
      try {
        val record = new Record()
        record.token = data(Token).toString.toLong
        record.timestamp = com.wajam.nrv.utils.timestamp.Timestamp(data(Timestamp).toString.toLong)
        val keys = data(Keys).asInstanceOf[Seq[String]]
        record.accessPath = new AccessPath(keys.map(new AccessKey(_)))
        Some(record)
      } catch {
        case e: Exception => {
          warn("Error creating Record for table {} from task context data {}: ", table.depthName("_"), data, e)
          None
        }
      }
    } else {
      None
    }
  }

  def loadMore() = {

    var transaction: MysqlTransaction = null
    try {
      val fromRecord = lastRecord
      currentRange = lastRecord match {
        case Some(record) => tokenRanges.find(_.contains(record.token))
        case None => {
          currentRange match {
            case Some(range) => range.nextRange(tokenRanges)
            case None => Some(tokenRanges.head)
          }
        }
      }

      transaction = storage.createStorageTransaction
      val recordsIterator = transaction.getAllLatest(table, rowsToFetch, currentRange.getOrElse(tokenRanges(0)), lastRecord)

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
        Keys -> record.accessPath.keys,
        Token -> record.token.toString,
        Value -> record.value,
        Timestamp -> record.timestamp
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

  def ack(data: Map[String, Any]) {
    // Update context with the latest acknowledged record data (excluding its value)
    context.data = (data - Value).map(entry => entry match {
      case (k, v: String) => (k, v)
      case (k, v: Seq[String]) => (k, v)
      case (k, v) => (k, v.toString)
    })
  }

  def kill() {}
}

object TableContinuousFeeder {
  val Keys = "keys"
  val Token = "token"
  val Value = "value"
  val Timestamp = "timestamp"
}
