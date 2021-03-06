package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.Implicits._
import org.scalatest.Matchers._
import com.wajam.spnl.TaskContext
import com.wajam.nrv.service.TokenRange
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.storage.mysql.FeederTestHelper._
import com.wajam.nrv.utils.timestamp.Timestamp
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class TestTableTimelineFeeder extends FunSuite with MysqlStorageFixture {

  test("timeline feeder should feed the table once in timestamp order") {
    withFixture { f =>
      val batchSize = 10

      // Batch with more records than feeder batch size
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        Seq.range(0, 20).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
      }, commit = true, onTimestamp = Timestamp(1))

      // 2nd batch with more record
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        Seq.range(0, 20).foreach(i => t1.set("b_%d".format(i), Map("key" -> "val")))
      }, commit = true, onTimestamp = Timestamp(2))

      // Exact same records count than batch size
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        Seq.range(0, batchSize).foreach(i => t1.set("c_%d".format(i), Map("key" -> "val")))
      }, commit = true, onTimestamp = Timestamp(3))

      // Extra individual records
      Seq.range(0, 20).foreach(i => {
        f.exec(t => {
          val t1 = t.from("mysql").from("table1")
          t1.set("d_%d".format(i), Map("key" -> "val"))
        }, commit = true, onTimestamp = Timestamp(i + 4))
      })

      val feeder = new TableTimelineFeeder("test", f.mysqlStorage, table1, List(TokenRange.All), batchSize)
      feeder.init(new TaskContext())
      val records = feeder.take(100).flatten.toList.map(_("new_timestamp").toString.toLong)

      records.size should be(70)
      records should be(records.sorted)
    }
  }

  test("timeline feeder should not load records more than once") {
    withFixture { f =>
      val batchSize = 10

      // Add a few records (less than batch size)
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        Seq.range(0, 4).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
      }, commit = true, onTimestamp = Timestamp(1))

      // Add a batch of more record (span first and second batch)
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        Seq.range(0, 8).foreach(i => t1.set("b_%d".format(i), Map("key" -> "val")))
      }, commit = true, onTimestamp = Timestamp(2))

      // Extra individual records
      Seq.range(0, 8).foreach(i => {
        f.exec(t => {
          val t1 = t.from("mysql").from("table1")
          t1.set("d_%d".format(i), Map("key" -> "val"))
        }, commit = true, onTimestamp = Timestamp(i + 4))
      })

      val feeder = new TableTimelineFeeder("test", f.mysqlStorage, table1, List(TokenRange.All), batchSize)
      feeder.init(new TaskContext())
      val records = feeder.take(100).flatten.toList.map(_("new_timestamp").toString.toLong)

      records.size should be(20)
      records should be(records.sorted)
    }
  }

  test("timeline feeder with ranges") {
    withFixture { f =>
      val batchSize = 10

      // Add records
      Seq.range(0, 40).foreach(i => {
        f.exec(t => {
          val t1 = t.from("mysql").from("table1")
          t1.set("key%d".format(i), Map("key" -> "val"))
        }, commit = true, onTimestamp = Timestamp(i + 4))
      })

      val ranges = List(TokenRange(1000000001L, 2000000000L), TokenRange(3000000001L, 4000000000L))

      val feeder = new TableTimelineFeeder("test", f.mysqlStorage, table1, ranges, batchSize)
      feeder.init(new TaskContext())
      val records = feeder.take(100).flatten.toList

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
  }

  test("should update context on ack") {
    withFixture { f =>
      // Add a few individual records
      Seq.range(0, 5).foreach(i => {
        f.exec(t => {
          val t1 = t.from("mysql").from("table1")
          t1.set("a_%d".format(i), Map("key" -> "val"))
        }, commit = true, onTimestamp = Timestamp(i + 1))
      })

      // Load all existing records
      val feeder1 = new TableTimelineFeeder("test", f.mysqlStorage, table1, List(TokenRange.All))
      feeder1.init(new TaskContext())
      var records1 = feeder1.take(100).flatten.toList
      records1.size should be(5)

      // Acknowledge the first record
      feeder1.ack(records1(0))

      // Create another feeder instance with a copy of the context, should resume from the first feeder context
      val feeder2 = new TableTimelineFeeder("test", f.mysqlStorage, table1, List(TokenRange.All))
      feeder2.init(feeder1.context.copy())
      var records2 = Iterator.continually({
        feeder2.next()
      }).take(100).flatten.toList
      records2.size should be(4)
      records2 should not contain(records1(0))
    }
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
    withFixture { f =>
      val batchSize = 10

      // Add a batch of records ( > batch size) with the same timestamp
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        0.until(batchSize * 2).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
      }, commit = true, onTimestamp = Timestamp(1))

      val feeder = new TableTimelineFeeder("test", f.mysqlStorage, table1, List(TokenRange.All), batchSize)
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
      f.exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("b", Map("key" -> "val"))
      }, commit = true, onTimestamp = Timestamp(2))

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

  test("should not return data beyong current consistent timestamp") {
    withFixture { f =>
      f.exec(t => {
        t.from("mysql").from("table1").set("key1", Map("k" -> "v"))
      }, commit = true, onTimestamp = 0L)

      f.exec(t => {
        t.from("mysql").from("table1").set("key2", Map("k" -> "v"))
      }, commit = true, onTimestamp = 100L)

      currentConsistentTimestamp = 50L

      val feeder = new TableTimelineFeeder("test", f.mysqlStorage, table1, List(TokenRange.All))
      feeder.init(new TaskContext())
      val records = feeder.take(100).flatten.toList

      records.size should be > 0
      val strKeys = records.map(_("keys").asInstanceOf[Seq[String]](0))
      strKeys.count(_ == "key1") should be(records.size)
      strKeys.count(_ == "key2") should be(0)
    }
  }
}
