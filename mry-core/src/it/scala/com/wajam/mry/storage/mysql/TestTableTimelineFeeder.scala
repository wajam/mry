package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.Implicits._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.spnl.TaskContext
import com.wajam.nrv.service.TokenRange
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTableTimelineFeeder extends TestMysqlBase {

  test("timeline feeder should feed the table once in timestamp order") {
    val batchSize = 10

    // Batch with more records than feeder batch size
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, 20).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(1))

    // 2nd batch with more record
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, 20).foreach(i => t1.set("b_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(2))

    // Exact same records count than batch size
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, batchSize).foreach(i => t1.set("c_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(3))

    // Extra individual records
    Seq.range(0, 20).foreach(i => {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("d_%d".format(i), Map("key" -> "val"))
      }, commit = true, onTimestamp = createTimestamp(i + 4))
    })

    val feeder = new TableTimelineFeeder("test", mysqlStorage, table1, List(TokenRange.All), batchSize)
    feeder.init(new TaskContext())
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList.map(_("new_timestamp").toString.toLong)

    records.size should be(70)
    records should be(records.sorted)
  }

  test("timeline feeder should not load records more than once") {
    val batchSize = 10

    // Add a few records (less than batch size)
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, 4).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(1))

    // Add a batch of more record (span first and second batch)
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, 8).foreach(i => t1.set("b_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(2))

    // Extra individual records
    Seq.range(0, 8).foreach(i => {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("d_%d".format(i), Map("key" -> "val"))
      }, commit = true, onTimestamp = createTimestamp(i + 4))
    })

    val feeder = new TableTimelineFeeder("test", mysqlStorage, table1, List(TokenRange.All), batchSize)
    feeder.init(new TaskContext())
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList.map(_("new_timestamp").toString.toLong)

    records.size should be(20)
    records should be(records.sorted)
  }

  test("timeline feeder with ranges") {
    val batchSize = 10

    // Add records
    Seq.range(0, 40).foreach(i => {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("key%d".format(i), Map("key" -> "val"))
      }, commit = true, onTimestamp = createTimestamp(i + 4))
    })

    val ranges = List(TokenRange(1000000001L, 2000000000L), TokenRange(3000000001L, 4000000000L))
    mysqlStorage.setLastConsistentTimestamp(Long.MaxValue, ranges)

    val feeder = new TableTimelineFeeder("test", mysqlStorage, table1, ranges, batchSize)
    feeder.init(new TaskContext())
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    records.size should be > 0
    records.size should be < 40
    val timestamps = records.map(_("new_timestamp").toString.toLong)
    timestamps should be(timestamps.sorted)

    // Verify all records tokens are from the expected ranges
    records.foreach(record => {
      val actualToken = record("token").toString.toLong
      ranges.exists(_.contains(actualToken)) should be(true)
    })
  }

  test("should update context on ack") {
    // Add a few individual records
    Seq.range(0, 5).foreach(i => {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("a_%d".format(i), Map("key" -> "val"))
      }, commit = true, onTimestamp = createTimestamp(i + 1))
    })

    // Load all existing records
    val feeder1 = new TableTimelineFeeder("test", mysqlStorage, table1, List(TokenRange.All))
    feeder1.init(new TaskContext())
    var records1 = Iterator.continually({
      feeder1.next()
    }).take(100).flatten.toList
    records1.size should be(5)

    // Acknowledge the first record
    feeder1.ack(records1(0))

    // Create another feeder instance with a copy of the context, should resume from the first feeder context
    val feeder2 = new TableTimelineFeeder("test", mysqlStorage, table1, List(TokenRange.All))
    feeder2.init(feeder1.context.copy())
    var records2 = Iterator.continually({
      feeder2.next()
    }).take(100).flatten.toList
    records2.size should be(4)
    records2 should not contain(records1(0))
  }

  test("context cache") {
    val name1 = "name1"
    val context1_1 = new TaskContext(data = Map("k1" -> 1))
    val context1_2 = new TaskContext(data = Map("k1" -> 2))
    val context1_3 = new TaskContext(data = Map("k1" -> 3))

    val name2 = "name2"
    val context2_1 = new TaskContext(data = Map("k2" -> 1))

    // Register
    TimelineFeederContextCache.register(name1, context1_1)
    TimelineFeederContextCache.register(name1, context1_2)
    TimelineFeederContextCache.register(name1, context1_3)
    TimelineFeederContextCache.register(name2, context2_1)
    TimelineFeederContextCache.contexts(name1) should be(List(context1_3, context1_2, context1_1))
    TimelineFeederContextCache.contexts(name2) should be(List(context2_1))
    TimelineFeederContextCache.contexts("unknown") should be(List[TaskContext]())

    // Unregister
    TimelineFeederContextCache.unregister(name1, context1_2)
    TimelineFeederContextCache.unregister(name2, context2_1)
    TimelineFeederContextCache.contexts(name1) should be(List(context1_3, context1_1))
    TimelineFeederContextCache.contexts(name2) should be(List[TaskContext]())

    // Unregister non-registered context
    TimelineFeederContextCache.unregister(name1, context2_1)
    TimelineFeederContextCache.unregister("unknown", context2_1)
    TimelineFeederContextCache.contexts(name1) should be(List(context1_3, context1_1))
    TimelineFeederContextCache.contexts(name2) should be(List[TaskContext]())
  }

  test("timeline feeder with single token over batch size should not load records more than once") {
    val batchSize = 10

    // Add a batch of records ( > batch size) with the same timestamp
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      0.until(batchSize * 2).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(1))

    val feeder = new TableTimelineFeeder("test", mysqlStorage, table1, List(TokenRange.All), batchSize)
    feeder.init(new TaskContext())

    // Verify the batch is loaded once
    val records = Iterator.continually({
      feeder.next().map(record => {
        feeder.ack(record)
        record
      })
    }).take(100).flatten.toList
    records.size should be(batchSize * 2)

    // Add an extra record
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      t1.set("b", Map("key" -> "val"))
    }, commit = true, onTimestamp = createTimestamp(2))

    // The extra record should be loaded
    val records2 = Iterator.continually({
      feeder.next().map(record => {
        feeder.ack(record)
        record
      })
    }).take(100).flatten.toList
    records2.size should be(1)
  }
}
