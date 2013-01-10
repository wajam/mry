package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.nrv.service.TokenRange

class TestTableContinuousFeeder extends TestMysqlBase {

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


    val feeder = new TableContinuousFeeder(mysqlStorage, table1, List(TokenRange.All))
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    // 91 = 100 - (5 loadMore + 4 end of records)
    records.size should be(91)

    val strKeys = records.map(_("keys").asInstanceOf[Seq[String]](0))
    strKeys.count(_ == "key17") should be(5)
    strKeys.count(_ == "key12") should be(5)
    strKeys.count(_ == "key15") should be(4) // last record
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

    val feeder = new TableContinuousFeeder(mysqlStorage, table1, ranges)
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    // Verify all records tokens are from the expected ranges
    records.foreach(record => {
        val actualToken = record("token").toString.toLong
        ranges.exists(_.contains(actualToken)) should be(true)
    })

    // unflattened records =
    //  None (loadMore), range1_records..., None (eor), None (loadMore), range2_records..., None (eor), None (loadMore), range1_records... ...
    records.size should be(71) // Trust me, the count should be 71
  }
}
