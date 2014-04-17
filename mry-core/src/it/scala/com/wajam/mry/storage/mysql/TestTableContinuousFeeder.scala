package com.wajam.mry.storage.mysql

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import com.wajam.nrv.service.{TokenRangeSeq, TokenRange}
import com.wajam.spnl.TaskContext
import org.mockito.Mockito._
import com.wajam.mry.storage.mysql.FeederTestHelper._

@RunWith(classOf[JUnitRunner])
class TestTableContinuousFeeder extends FunSuite {

  /**
   * Test feeder which use token as record. When loading records, it simply enumerates all the tokens in the
   * specified token range.
   */
  class ContinuousTokenFeeder(tokenRanges: TokenRangeSeq, limit: Int)
    extends TokenFeeder(tokenRanges, limit) with TableContinuousFeeder

  test("feeder should loop when loaded all data - large load limit") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 1000)

    feeder.completedCount.clear()
    feeder.completedCount.count should be(0)

    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
    feeder.completedCount.count should be(feeder.firstRangeLoadCount)
  }

  test("feeder should loop when loaded all data - small load limit") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 2)

    feeder.completedCount.clear()
    feeder.completedCount.count should be(0)

    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
    feeder.completedCount.count should be(feeder.firstRangeLoadCount)
  }

  test("should resume from context position") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext(Map("token" -> 11)))
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(12, 2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12))
  }

  test("should start from beginning when out of range context position") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext(Map("token" -> 8)))
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
  }

  test("should start from beginning when context is empty") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12, 2))
  }

  test("should resume from same position if load fail") {
    val ranges = List(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 2)
    val spyFeeder = spy(feeder)

    when(spyFeeder.loadRecords(TokenRange(10, 12), Some(11L))).thenThrow(new RuntimeException())
    val recordsWithError = spyFeeder.take(50).flatten.toList
    recordsWithError.flatMap(feeder.toRecord) should be(List(2, 3, 4, 5, 10, 11))

    reset(spyFeeder)
    val records = spyFeeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(12, 2, 3, 4, 5, 10, 11, 12, 2, 3, 4, 5, 10, 11, 12))
  }

  test("should save last ack position in context") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new ContinuousTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    feeder.ack(feeder.fromRecord(11L))
    feeder.toRecord(feeder.context.data) should be(Some(11L))
  }
}
