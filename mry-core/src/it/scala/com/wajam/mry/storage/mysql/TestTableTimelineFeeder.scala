package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.Implicits._
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.spnl.TaskContext

class TestTableTimelineFeeder extends TestMysqlBase {

  // TODO: test when Manu will fix timeline feeder with timestamps
  ignore("timeline feeder should feed the table once in timestamp order") {
    exec(t => {
      val t1 = t.from("mysql").from("table1")
      Seq.range(0, 20).foreach(i => t1.set("a_%d".format(i), Map("key" -> "val")))
    }, commit = true, onTimestamp = createTimestamp(1))

    Seq.range(0, 20).foreach(i => {
      exec(t => {
        val t1 = t.from("mysql").from("table1")
        t1.set("b_%d".format(i), Map("key" -> "val"))
      }, commit = true, onTimestamp = createTimestamp(i + 2))
    })

    val feeder = new TableTimelineFeeder(mysqlStorage, table1, 10)
    feeder.init(new TaskContext())
    val records = Iterator.continually({
      feeder.next()
    }).take(100).flatten.toList

    records.size should be(40)
  }

}
