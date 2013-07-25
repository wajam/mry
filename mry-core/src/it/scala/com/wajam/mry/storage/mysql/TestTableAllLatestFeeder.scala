package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.TaskContext
import com.wajam.mry.storage.mysql.TableAllLatestFeeder._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTableAllLatestFeeder extends TestMysqlBase {

  test("continuous feeder should feed in loop a table and in token+key order") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
    }, commit = true, onTimestamp = createTimestamp(0))


    val feeder = new TableAllLatestFeeder("test", mysqlStorage, table1, List(TokenRange.All)) with TableContinuousFeeder
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    // 91 = 100 - (5 loadMore + 4 end of records)
    records.size should be(91)

    val strKeys = records.map(_(Keys).asInstanceOf[Seq[String]](0))
    strKeys.count(_ == "key17") should be(5)
    strKeys.count(_ == "key12") should be(5)
    strKeys.count(_ == "key15") should be(4) // last record
  }

  test("init with context") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    for (((token, key), i) <- keys.zipWithIndex) {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set(key, Map(key -> key))
      }, commit = true, onTimestamp = createTimestamp(i))
    }

    // Should load records starting from context data record
    val feeder = new TableAllLatestFeeder("test", mysqlStorage, table1, List(TokenRange.All)) with TableContinuousFeeder
    val feederContext = new TaskContext()
    feederContext.data += (Token -> keys(5)._1)
    feederContext.data += (Keys -> Seq(keys(5)._2))
    feederContext.data += (Timestamp -> createTimestamp(5))
    feeder.init(feederContext)
    val records = Iterator.continually({
      feeder.next()
    }).take(10).flatten.toList

    records(0)(Token) should be(keys(6)._1.toString)
  }

  test("init with context invalid data") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    for (((token, key), i) <- keys.zipWithIndex) {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set(key, Map(key -> key))
      }, commit = true, onTimestamp = createTimestamp(i))
    }

    // Should load records from start
    val feeder = new TableAllLatestFeeder("test", mysqlStorage, table1, List(TokenRange.All)) with TableContinuousFeeder
    val feederContext = new TaskContext()
    feederContext.data += (Token -> keys(5))
    feederContext.data += (Keys -> Seq(keys(5)._2))
    feederContext.data += (Timestamp -> "abc") // Invalid timestamp
    feeder.init(feederContext)
    val records = Iterator.continually({
      feeder.next()
    }).take(10).flatten.toList

    records(0)(Token) should be(keys(0)._1.toString)
  }

  test("init with empty context") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    for (((token, key), i) <- keys.zipWithIndex) {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set(key, Map(key -> key))
      }, commit = true, onTimestamp = createTimestamp(i))
    }

    // Should load records from start
    val feeder = new TableAllLatestFeeder("test", mysqlStorage, table1, List(TokenRange.All)) with TableContinuousFeeder
    feeder.init(new TaskContext())
    val records = Iterator.continually({
      feeder.next()
    }).take(10).flatten.toList

    records(0)(Token) should be(keys(0)._1.toString)
  }

  test("save and resume context with multi level table") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    for (((token, key), i) <- keys.zipWithIndex) {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set(key, Map(key -> key))
        t1.get(key).from("table1_1").set(key, Map(key -> key))
      }, commit = true, onTimestamp = createTimestamp(i))
    }

    // Load some records
    val feeder1 = new TableAllLatestFeeder("test", mysqlStorage, table1_1, List(TokenRange.All)) with TableContinuousFeeder
    feeder1.init(new TaskContext())
    val records = Iterator.continually({
      feeder1.next()
    }).take(10).flatten.toList

    records.size should be(9)
    records.map(_(Token)) should be(keys.slice(0, records.length).map(_._1.toString))

    // Acknowledge the last records
    feeder1.ack(records.last)

    // Create another feeder instance with a copy of the context, should resume from the previous feeder context
    val feeder2 = new TableAllLatestFeeder("test", mysqlStorage, table1_1, List(TokenRange.All)) with TableContinuousFeeder
    val context2 = new TaskContext()
    context2.updateFromJson(feeder1.context.toJson)
    feeder2.init(context2)
    var records2 = Iterator.continually({
      feeder2.next()
    }).take(10).flatten.toList

    records2.size should be(9)
    records2.map(_(Token)) should be(keys.slice(records.length, records.length + records2.length).map(_._1.toString))
  }

  test("continuous feeder should feed in loop a table and in token+key order for ranges") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
    }, commit = true, onTimestamp = createTimestamp(0))

    val ranges = List(TokenRange(1000000001L, 2000000000L), TokenRange(3000000001L, 4000000000L))
    val expectedKeys = keys.filter(k => ranges.exists(_.contains(k._1))).toList

    val feeder = new TableAllLatestFeeder("test", mysqlStorage, table1, ranges, loadLimit = 3) with TableContinuousFeeder
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    val expectedTokens = Stream.continually(expectedKeys.map(_._1)).flatten
    val actualTokens = records.map(_(Token).toString.toLong)

    actualTokens.size should be > 50
    actualTokens should be(expectedTokens.take(actualTokens.size).toList)
  }

  test("should not return data beyond current consistent timestamp") {
    exec(t => {
      t.from("mysql").from("table1").set("key1", Map("k" -> "v"))
    }, commit = true, onTimestamp = 0L)

    exec(t => {
      t.from("mysql").from("table1").set("key2", Map("k" -> "v"))
    }, commit = true, onTimestamp = 100L)

    currentConsistentTimestamp = 50L

    val feeder = new TableAllLatestFeeder("test", mysqlStorage, table1, List(TokenRange.All)) with TableContinuousFeeder
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    records.size should be > 0
    val strKeys = records.map(_(Keys).asInstanceOf[Seq[String]](0))
    strKeys.count(_ == "key1") should be(records.size)
    strKeys.count(_ == "key2") should be(0)

  }
}
