package com.wajam.mry.storage.mysql

import com.wajam.commons.Logging
import com.wajam.nrv.service.{TokenRangeSeq, TokenRange}
import com.wajam.mry.execution.{NullValue, Value}
import scala.annotation.tailrec
import com.wajam.spnl.TaskContext.ContextData
import com.wajam.spnl.feeder.Feeder.FeederData

/**
 * Fetches all current defined (not null) data on a table.
 */
abstract class TableAllLatestFeeder(val name: String, storage: MysqlStorage, table: Table,
                                    val tokenRanges: TokenRangeSeq, loadLimit: Int = 1000)
  extends ResumableRecordDataFeeder with Logging {

  import TableAllLatestFeeder._

  type DataRecord = Record

  def loadRecords(range: TokenRange, startAfterRecord: Option[Record]) = {
    val transaction = storage.createStorageTransaction
    try {
      val records = filterStartRecord(
        transaction.getAllLatest(table, loadLimit, range, startAfterRecord).toIterable, startAfterRecord).toList
      if (records.isEmpty) {
        // No records found! We don't know if we have reach the end of the table or if the number of consecutive
        // deleted records is larger than the loadLimit. Fallback to a slower but deterministic method.
        // Load all following records including deleted records until we reach a non deleted record or the end of the table
        loadFirstNonDeletedRecords(range, startAfterRecord, transaction)
      } else {
        records
      }
    } finally {
      transaction.commit()
    }
  }

  @tailrec
  private def loadFirstNonDeletedRecords(range: TokenRange, startAfterRecord: Option[Record],
                                         transaction: MysqlTransaction): Option[Record] = {
    val records = filterStartRecord(
      transaction.getAllLatest(table, loadLimit, range, startAfterRecord, includeDeleted = true).toIterable, startAfterRecord)
    if (records.isEmpty) {
      // Stop here, we have reach end of table
      None
    } else {
      // Returns first non deleted record or continue further
      records.collectFirst{case record if !record.value.isNull => record} match {
        case record@Some(_) => record
        case None => loadFirstNonDeletedRecords(range, records.lastOption, transaction)
      }
    }
  }

  def token(record: Record) = record.token

  def toRecord(data: FeederData) = {
    if (data.contains(Keys) && data.contains(Token) && data.contains(Timestamp))
    {
      try {
        val token = data(Token).toString.toLong
        val timestamp = com.wajam.nrv.utils.timestamp.Timestamp(data(Timestamp).toString.toLong)
        val keys = data(Keys).asInstanceOf[Seq[String]]
        val accessPath = new AccessPath(keys.map(new AccessKey(_)))
        val value = data.get(Value).getOrElse(NullValue).asInstanceOf[Value]
        Some(new Record(table, value, token, accessPath, timestamp = timestamp))
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

  def fromRecord(record: Record): FeederData = {
    Map(Keys -> record.accessPath.keys,
        Token -> record.token.toString,
        Value -> record.value,
        Timestamp -> record.timestamp)
  }

  override def toContextData(data: FeederData): ContextData = data - Value
}

object TableAllLatestFeeder {
  val Keys = "keys"
  val Token = "token"
  val Value = "value"
  val Timestamp = "timestamp"
}
