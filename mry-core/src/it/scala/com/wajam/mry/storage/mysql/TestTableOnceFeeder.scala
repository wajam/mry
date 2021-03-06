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
class TestTableOnceFeeder extends FunSuite {

  /**
   * Test feeder which use token as record. When loading records, it simply enumerates all the tokens in the
   * specified token range.
   */
  class OnceTokenFeeder(tokenRanges: TokenRangeSeq, limit: Int)
    extends TokenFeeder(tokenRanges, limit) with TableOnceFeeder

  test("should resume from context position") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext(Map("token" -> 3)))
    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(4, 5, 10, 11, 12))
    feeder.isProcessing should be(false)
  }

  test("should start from beginning when out of range context position") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext(Map("token" -> 8)))
    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))
    feeder.isProcessing should be(false)
  }

  test("should start from beginning when context is empty") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))
    feeder.isProcessing should be(false)
  }

  test("should resume from same position if load fail") {
    val ranges = List(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 2)
    feeder.init(TaskContext())
    feeder.start("") should be(true)
    val spyFeeder = spy(feeder)

    when(spyFeeder.loadRecords(TokenRange(10, 12), Some(11L))).thenThrow(new RuntimeException())
    feeder.isProcessing should be(true)
    val recordsWithError = spyFeeder.take(50).flatten.toList
    recordsWithError.flatMap(feeder.toRecord) should be(List(2, 3, 4, 5, 10, 11))
    feeder.isProcessing should be(true)

    reset(spyFeeder)
    val records = spyFeeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(12))
    feeder.isProcessing should be(false)
  }

  test("should save last ack position in context") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    feeder.ack(feeder.fromRecord(11L))
    feeder.toRecord(feeder.context.data) should be(Some(11L))
  }

  test("feeder should not loop when loaded all data - large load limit") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 1000)
    feeder.init(TaskContext())
    feeder.start("") should be(true)

    feeder.isProcessing should be(true)
    val records = feeder.take(50).flatten.toList
    feeder.isProcessing should be(false)
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))
  }

  test("feeder should not loop when loaded all data - small load limit") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 2)
    feeder.init(TaskContext())
    feeder.start("") should be(true)

    feeder.isProcessing should be(true)
    val records = feeder.take(50).flatten.toList
    feeder.isProcessing should be(false)
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))
  }

  test("starting againg should not return results") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    val records = feeder.take(5).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5))
    feeder.isProcessing should be(true)
    feeder.stop() should be(true)
    feeder.isProcessing should be(false)
    Thread.sleep(1000)

    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    val records2 = feeder.take(50).flatten.toList
    feeder.isProcessing should be(false)
    records2.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))

  }

  test("should be stopped automatically when iteration completed") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 10)
    feeder.init(TaskContext())
    feeder.start("") should be(true)
    feeder.start("") should be(false)
    feeder.isProcessing should be(true)
    val records = feeder.take(50).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))

    feeder.isProcessing should be(false)
    Thread.sleep(1000)
    feeder.start("") should be(true)
    feeder.start("") should be(false)
    feeder.isProcessing should be(true)
    val records2 = feeder.take(50).flatten.toList
    records2.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10, 11, 12))
    feeder.isProcessing should be(false)
  }

  test("should not be able to fetch results when feeder is stopped") {
    val ranges = Seq(TokenRange(2, 5), TokenRange(10, 12))
    val feeder = new OnceTokenFeeder(ranges, limit = 1)
    feeder.init(TaskContext())
    feeder.start("") should be(true)
    feeder.isProcessing should be(true)
    val records = feeder.take(11).flatten.toList
    records.flatMap(feeder.toRecord).take(15) should be(List(2, 3, 4, 5, 10))
    feeder.isProcessing should be(true)
    feeder.stop() should be(true)
    val records2 = feeder.take(5).flatten.toList
    records2.flatMap(feeder.toRecord).take(15) should be(List())
    feeder.isProcessing should be(false)
  }
}
