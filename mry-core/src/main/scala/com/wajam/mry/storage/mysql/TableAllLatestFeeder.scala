package com.wajam.mry.storage.mysql

import com.wajam.nrv.Logging
import com.wajam.spnl.feeder.CachedDataFeeder
import com.wajam.nrv.service.TokenRange

/**
 * Fetches all current defined (not null) data on a table.
 */
abstract class TableAllLatestFeeder(val name: String, storage: MysqlStorage, table: Table,
                                    val tokenRanges: Seq[TokenRange], loadLimit: Int = 1000)
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
        val record = new Record(table)
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

  def toData(record: Record) = {
    Map(Keys -> record.accessPath.keys,
      Token -> record.token.toString,
      Value -> record.value,
      Timestamp -> record.timestamp)
  }

  override def ack(data: Map[String, Any]) {
    // Exclude value from context
    super.ack(data - Value)
  }
}

object TableAllLatestFeeder {
  val Keys = "keys"
  val Token = "token"
  val Value = "value"
  val Timestamp = "timestamp"
}
