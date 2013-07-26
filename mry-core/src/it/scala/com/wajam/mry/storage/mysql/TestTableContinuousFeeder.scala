package com.wajam.mry.storage.mysql

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.service.{TokenRangeSeq, TokenRange}
import com.wajam.spnl.TaskContext
import org.mockito.Mockito._

@RunWith(classOf[JUnitRunner])
class TestTableContinuousFeeder extends FunSuite {

  /**
   * Test feeder which use token as record. When loading records, it simply enumerates all the tokens in the
   * specified token range.
   */
  class ContinuousTokenFeeder(val tokenRanges: TokenRangeSeq, limit: Int)
    extends ResumableRecordDataFeeder with TableContinuousFeeder {

    type DataRecord = Long

    val name = "ContinuousTokenFeeder"

    def token(record: Long) = record

    def loadRecords(range: TokenRange, fromRecord: Option[Long]) = {
      (fromRecord match {
        case Some(start) => start.to(range.end)
        case None => range.start.to(range.end)
      }).take(limit)
    }

    def toRecord(data: Map[String, Any]) = data.get("token").map(_.toString.toLong)

    def fromRecord(record: Long) = Map("token" -> record)
  }

  test("feeder should loop when loaded all data - large load limit") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 1000)
    val records = Iterator.continually(feeder.next()).take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be (List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
  }

  test("feeder should loop when loaded all data - small load limit") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 2)
    val records = Iterator.continually(feeder.next()).take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be (List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
  }

  test("should resume from context position") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext(Map("token" -> 11)))
    val records = Iterator.continually(feeder.next()).take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be (List(12, 2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12))
  }

  test("should start from beginning when out of range context position") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext(Map("token" -> 8)))
    val records = Iterator.continually(feeder.next()).take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be (List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
  }

  test("should start from beginning when context is empty") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    val records = Iterator.continually(feeder.next()).take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be (List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
  }

  test("should resume from same position if load fail") {
    val ranges = List(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 2)
    val spyFeeder = spy(feeder)

    when(spyFeeder.loadRecords(TokenRange(10, 12), Some(11L))).thenThrow(new RuntimeException())
    val recordsWithError = Iterator.continually(spyFeeder.next()).take(50).flatten.toList
    recordsWithError.flatMap(feeder.toRecord) should be (List(2, 3, 4, 5, 10, 11))

    reset(spyFeeder)
    val records = Iterator.continually(spyFeeder.next()).take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be (List(12, 2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12))
  }

  test("should save last ack position in context") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    feeder.ack(feeder.fromRecord(11L))
    feeder.toRecord(feeder.context.data) should be (Some(11L))
  }
}
