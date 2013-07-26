package com.wajam.mry.storage.mysql

import com.wajam.nrv.utils.CurrentTime
import com.wajam.nrv.Logging
import com.wajam.nrv.service.{TokenRangeSeq, TokenRange}

/**
 * Fetches tombstone records (i.e. with null value) from specified table.
 */
abstract class TableTombstoneFeeder(val name: String, storage: MysqlStorage, table: Table,
                                    val tokenRanges: TokenRangeSeq, minTombstoneAgeMs: Long, loadLimit: Int = 50)
  extends ResumableRecordDataFeeder with CurrentTime with Logging {

  import TableTombstoneFeeder._

  type DataRecord = TombstoneRecord

  def loadRecords(range: TokenRange, fromRecord: Option[TombstoneRecord]): Iterable[TombstoneRecord] = {
    val transaction = storage.createStorageTransaction
    try {
      transaction.getTombstoneRecords(table, loadLimit, range, currentTime - minTombstoneAgeMs, fromRecord)
    } finally {
      transaction.commit()
    }
  }

  def token(record: TombstoneRecord) = record.token

  def toRecord(data: Map[String, Any]): Option[TombstoneRecord] = {
    if (data.contains(Keys) && data.contains(Token) && data.contains(Timestamp))
    {
      try {
        val token = data(Token).toString.toLong
        val timestamp = com.wajam.nrv.utils.timestamp.Timestamp(data(Timestamp).toString.toLong)
        val keys = data(Keys).asInstanceOf[Seq[String]]
        val accessPath = new AccessPath(keys.map(new AccessKey(_)))
        Some(new TombstoneRecord(table, token, accessPath, timestamp)
        )
      } catch {
        case e: Exception => {
          warn("Error creating record for table {} from task context data {}: ", table.depthName("_"), data, e)
          None
        }
      }
    } else {
      None
    }
  }

  def toData(record: TombstoneRecord): Map[String, Any] = {
    Map(Keys -> record.accessPath.keys, Token -> record.token, Timestamp -> record.timestamp)
  }
}

object TableTombstoneFeeder {
  val Keys = "keys"
  val Token = "token"
  val Timestamp = "timestamp"
}