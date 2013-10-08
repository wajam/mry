package com.wajam.mry.storage.mysql

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.mry.execution.Implicits._
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.commons.ControlableCurrentTime
import com.wajam.spnl.TaskData

@RunWith(classOf[JUnitRunner])
class TestTableTombstoneFeeder extends TestMysqlBase {

  test("should extract token from tombstone record") {
    val table = table1
    val ranges = List(TokenRange.All)
    val feeder = new TableTombstoneFeeder("test", mysqlStorage, table, ranges, 0) with TableContinuousFeeder
    val record = TombstoneRecord(table, 101L, AccessPath(Seq(AccessKey("k"))), 12L)
    feeder.token(record) should be(101L)
  }

  test("should convert record to data and data to record") {
    import TableTombstoneFeeder.{Keys => KEYS, Token => TOKEN, Timestamp => TIMESTAMP}

    val feeder = new TableTombstoneFeeder("test", mysqlStorage, table1_1, Seq(TokenRange.All), 0) with TableContinuousFeeder
    val expectedData = TaskData(token = 101, values = Map(KEYS -> Seq("k1", "k2"), TIMESTAMP -> Timestamp(12L)))
    val record = feeder.toRecord(expectedData)
    record should not be None
    feeder.fromRecord(record.get) should be(expectedData)
  }

  test("should load tombstone records") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2"))
      table.set("key3", Map("k" -> "value3"))
      table.get("key1").from("table1_1").set("key1.1", Map("k" -> "value1.1"))
      table.get("key2").from("table1_1").set("key2.1a", Map("k" -> "value2.1a"))
      table.get("key2").from("table1_1").set("key2.1b", Map("k" -> "value2.1b"))
      table.get("key2").from("table1_1").set("key2.1c", Map("k" -> "value2.1c"))
      table.get("key2").from("table1_1").set("key2.1d", Map("k" -> "value2.1d"))
      table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
    }, commit = true, onTimestamp = Timestamp(100))

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("key2")
    }, commit = true, onTimestamp = Timestamp(200))

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("key3")
    }, commit = true, onTimestamp = Timestamp(300))


    var currentConsistentTimestamp = Timestamp(1500)
    mysqlStorage.setCurrentConsistentTimestamp((range) => currentConsistentTimestamp)

    val feeder = new TableTombstoneFeeder("test", mysqlStorage, table1_1, Seq(TokenRange.All),
      minTombstoneAge = 1000) with TableContinuousFeeder with ControlableCurrentTime

    val all = feeder.loadRecords(TokenRange.All, None)
    all.size should be(5)
    all.map(_.accessPath.keys.last).toSet should be(Set("key2.1a", "key2.1b", "key2.1c", "key2.1d", "key3.1"))

    // Read up to timestamp 200 i.e. currentConsistentTimestamp - minTombstoneAge (1200 - 1000)
    currentConsistentTimestamp = Timestamp(1200)

    val key2All = feeder.loadRecords(TokenRange.All, None)
    key2All.size should be(4)
    key2All.map(_.accessPath.keys.last) should be(Seq("key2.1a", "key2.1b", "key2.1c", "key2.1d"))

    val limitedFeeder = new TableTombstoneFeeder("test", mysqlStorage, table1_1, Seq(TokenRange.All),
      minTombstoneAge = 1000, loadLimit = 3) with TableContinuousFeeder with ControlableCurrentTime

    val key2First3 = limitedFeeder.loadRecords(TokenRange.All, None)
    key2First3.size should be(3)
    key2First3.map(_.accessPath.keys.last) should be(Seq("key2.1a", "key2.1b", "key2.1c"))

    val key2Reminder = limitedFeeder.loadRecords(TokenRange.All, Some(key2First3.last))
    key2Reminder.size should be(1)
    key2Reminder.map(_.accessPath.keys.last) should be(Seq("key2.1d"))
  }

}
