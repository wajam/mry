package com.wajam.mry.storage.mysql

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable
import com.wajam.spnl.feeder.Feeder.FeederData
import scala.language.implicitConversions
import com.wajam.nrv.service.{TokenRange, TokenRangeSeq}

object FeederTestHelper {
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[FeederData]] with Closable = {

    new Iterator[Option[FeederData]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }

  /**
   * Test feeder which use token as record. When loading records, it simply enumerates all the tokens in the
   * specified token range.
   */
  abstract class TokenFeeder(val tokenRanges: TokenRangeSeq, limit: Int)
    extends ResumableRecordDataFeeder {

    var firstRangeLoadCount = 0

    type DataRecord = Long

    val name = "TokenFeeder"

    def token(record: Long) = record

    def loadRecords(range: TokenRange, startAfterRecord: Option[Long]) = {
      firstRangeLoadCount += (if (range == tokenRanges.head && startAfterRecord.isEmpty) 1 else 0)

      (startAfterRecord match {
        case Some(start) => (start + 1).to(range.end)
        case None => range.start.to(range.end)
      }).take(limit)
    }

    def toRecord(data: FeederData) = data.get("token").map(_.toString.toLong)

    def fromRecord(record: Long) = Map("token" -> record)
  }

}