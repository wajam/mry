package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.nrv.service.{TokenRangeSeq, TokenRange}
import com.wajam.mry.execution.{NullValue, Value}

/**
 * Fetches all current defined (not null) data on a table.
 */
abstract class TableAllLatestFeeder(val name: String, storage: MysqlStorage, table: Table,
                                    val tokenRanges: TokenRangeSeq, loadLimit: Int = 1000)
  extends ResumableRecordDataFeeder with Logging {

  import TableAllLatestFeeder._

  type DataRecord = Record

  def loadRecords(range: TokenRange, fromRecord: Option[Record]) = {
    val transaction = storage.createStorageTransaction
    try {
      transaction.getAllLatest(table, loadLimit, range, fromRecord).toList
    } finally {
      transaction.commit()
    }
  }

  def token(record: Record) = record.token

  def toRecord(data: Map[String, Any]) = {
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

  def fromRecord(record: Record) = {
    Map(Keys -> record.accessPath.keys,
      Token -> record.token.toString,
      Value -> record.value,
      Timestamp -> record.timestamp)
  }

  override def toContextData(data: Map[String, Any]): Map[String, Any] = data - Value
}

object TableAllLatestFeeder {
  val Keys = "keys"
  val Token = "token"
  val Value = "value"
  val Timestamp = "timestamp"
}
