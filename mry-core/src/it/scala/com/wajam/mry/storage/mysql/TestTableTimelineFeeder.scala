package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.Implicits._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.spnl.TaskContext
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.wajam.nrv.utils.ControlableCurrentTime


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

    val feeder = new TableTimelineFeeder(mysqlStorage, table1, batchSize)
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

    val feeder = new TableTimelineFeeder(mysqlStorage, table1, batchSize)
    feeder.init(new TaskContext())
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList.map(_("new_timestamp").toString.toLong)

    records.size should be(20)
    records should be(records.sorted)
  }

  test("empty timeline feeder should not call database during throttling period") {
    val spyStorage = spy(mysqlStorage)
    val throttlePeriod = 1000
    val feeder = new TableTimelineFeeder(spyStorage, table1, emptyThrottlePeriod = throttlePeriod) with ControlableCurrentTime
    feeder.init(new TaskContext())

    // Ensure storage called once during throttling period
    var records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList
    records.size should be(0)
    verify(spyStorage, times(1)).createStorageTransaction

    // Ensure storage is called once after throttling period
    feeder.advanceTime(throttlePeriod + 1)
    records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList
    records.size should be(0)
    verify(spyStorage, times(2)).createStorageTransaction
  }

  test("empty timeline feeder should read new inserted records only after throttling period expired") {
    // Initialize table with a few records
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, 5).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(0))

    // Load all existing records
    val throttlePeriod = 1000
    val feeder = new TableTimelineFeeder(mysqlStorage, table1, emptyThrottlePeriod = throttlePeriod) with ControlableCurrentTime
    feeder.init(new TaskContext())
    var records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList
    records.size should be(5)

    // Add extra individual records
    Seq.range(0, 5).foreach(i => {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("b_%d".format(i), Map("key" -> "val"))
      }, commit = true, onTimestamp = createTimestamp(i + 2))
    })

    // Fetch again without advancing time, should be empty because of throttling period
    records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList
    records.size should be(0)

    // Finally fetch after advancing time
    feeder.advanceTime(throttlePeriod * 2)
    records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList
    records.size should be(5)
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
    val feeder1 = new TableTimelineFeeder(mysqlStorage, table1)
    feeder1.init(new TaskContext())
    var records1 = Iterator.continually({
      feeder1.next()
    }).take(100).flatten.toList
    records1.size should be(5)

    // Acknowledge the first record
    feeder1.ack(records1(0))

    // Create another feeder instance with a copy of the context, should resume from the first feeder context
    val feeder2 = new TableTimelineFeeder(mysqlStorage, table1)
    feeder2.init(feeder1.context.copy())
    var records2 = Iterator.continually({
      feeder2.next()
    }).take(100).flatten.toList
    records2.size should be(4)
    records2 should not contain(records1(0))
  }

}
