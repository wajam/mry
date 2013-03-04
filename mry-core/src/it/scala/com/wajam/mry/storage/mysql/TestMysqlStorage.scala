package com.wajam.mry.storage.mysql

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import com.wajam.mry.storage.StorageException
import collection.mutable
import util.Random
import org.scalatest.matchers.ShouldMatchers._
import com.wajam.mry.storage.mysql.TimelineSelectMode.AtTimestamp
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.TimestampIdGenerator

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends TestMysqlBase {

  test("should get committed record") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("mapk" -> toVal("value1")))
    }, commit = true)

    val Seq(v) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      t.ret(table.get("key1"))
    }, commit = false)

    v.value.serializableValue match {
      case m: MapValue =>
        assert(m("mapk").equalsValue("value1"))
      case _ =>
        fail("Didn't receive a map")
    }
  }

  test("a get before a set should return initial value, not new value") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("mapk" -> toVal("value1")))
      table.set("key2", Map("mapk" -> toVal("value2")))
    }, commit = true)

    val Seq(rec1, rec2, rec3, rec4) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      val a = table.get("key1")
      val b = table.get("key2")
      table.set("key1", Map("mapk" -> toVal("value1.2")))
      table.delete("key2")
      val c = table.get("key1")
      val d = table.get("key2")
      t.returns(a, b, c, d)
    }, commit = true)


    val val1 = rec1.asInstanceOf[MapValue].mapValue("mapk")
    val val2 = rec2.asInstanceOf[MapValue].mapValue("mapk")
    val val3 = rec3.asInstanceOf[MapValue].mapValue("mapk")
    assert(rec4.equalsValue(NullValue))

    assert(val1.equalsValue("value1"), val1)
    assert(val2.equalsValue("value2"), val2)
    assert(val3.equalsValue("value1.2"), val3)
  }

  test("a get before a delete should return initial value, not null") {
    exec(t => {
      t.from("mysql").from("table1").set("key1", Map("mapk" -> toVal("value1")))
    }, commit = true)

    val Seq(rec1, rec2) = exec(t => {
      val table = t.from("mysql").from("table1")
      val a = table.get("key1")
      table.delete("key1")
      val b = table.get("key1")
      t.returns(a, b)
    }, commit = true)

    val val1 = rec1.asInstanceOf[MapValue].mapValue("mapk")
    assert(rec2.equalsValue(NullValue))
    assert(val1.equalsValue("value1"), val1)
  }

  test("a get before a set on multi level hierarchy should return initial values, not new values") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("mapk" -> toVal("value1")))
      val rec1 = table.get("key1")
      rec1.from("table1_1").set("key1_1", Map("mapk" -> toVal("value1_1a")))
      rec1.from("table1_1").set("key1_2", Map("mapk" -> toVal("value1_2a")))
      rec1.from("table1_1").set("key1_3", Map("mapk" -> toVal("value1_3a")))

    }, commit = true)

    val Seq(rec1, rec2) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1").get("key1").from("table1_1")
      val a = table.get()
      table.delete("key1_1")
      table.set("key1_2", Map("mapk" -> toVal("value1_2b")))
      val b = table.get()
      t.returns(a, b)
    }, commit = true)

    val val1 = rec1.asInstanceOf[ListValue]
    val val2 = rec2.asInstanceOf[ListValue]
    val1.listValue.size should be(3)
    val2.listValue.size should be(2)
    val2.listValue(1).asInstanceOf[MapValue].mapValue("mapk").equalsValue("value1_2b")
  }


  test("shouldn't get uncommited record") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key2", Map("mapk" -> toVal("value2")))
    }, commit = false)

    val Seq(v) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      t.ret(table.get("key2"))
    }, commit = false)
    assert(v.value.serializableValue.equalsValue(new NullValue))
  }

  test("should support multi hierarchy table") {
    val Seq(record1, record2, record3) = exec(t => {
      val storage = t.from("mysql")
      val table1 = storage.from("table1")
      table1.set("k1", Map("k" -> "v1"))

      val record1 = table1.get("k1")
      val table1_1 = record1.from("table1_1")
      table1_1.set("k1.2", Map("k" -> "v1.2"))
      val record2 = table1_1.get("k1.2")

      table1_1.get("k1.2").from("table1_1_1").set("k1.2.1", Map("k" -> "v1.2.1"))
      val record3 = table1_1.get("k1.2").from("table1_1_1").get("k1.2.1")

      t.ret(record1, record2, record3)
    }, commit = true)

    assert(record1.asInstanceOf[MapValue].mapValue("k").equalsValue("v1"))
    assert(record2.asInstanceOf[MapValue].mapValue("k").equalsValue("v1.2"))
    assert(record3.asInstanceOf[MapValue].mapValue("k").equalsValue("v1.2.1"))
  }

  test("shouldn't be able to get multiple rows on first depth table") {
    intercept[InvalidParameter] {
      val Seq(records) = exec(t => {
        t.ret(t.from("mysql").from("table1").get())
      })
    }
  }

  test("should be able to get multiple rows on second and third depth table") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2"))
      table.set("key3", Map("k" -> "value3"))
      table.delete("key1")
      table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
      table.get("key3").from("table1_1").set("key3.2", Map("k" -> "value3.2"))
      table.get("key3").from("table1_1").get("key3.1").from("table1_1_1").set("key3.1.1", Map("k" -> "value3.1.1"))
      table.get("key3").from("table1_1").get("key3.1").from("table1_1_1").set("key3.1.2", Map("k" -> "value3.1.2"))
      table.get("key3").from("table1_1").get("key3.2").from("table1_1_1").set("key3.3.1", Map("k" -> "value3.3.1"))
    }, commit = true)

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.get("key2").from("table1_1").set("key2.1", Map("k" -> "value2.1"))
      table.get("key3").from("table1_1").set("key3.3", Map("k" -> "value3.3"))
    }, commit = true)


    // shouldn't be able to get a record on second hiearchy if first hierarchy record doesn't exist
    intercept[StorageException] {
      exec(t => {
        val rec1 = t.from("mysql").from("table1").get("key1").from("table1_1").get()
        t.ret(rec1)
      })
    }

    // shouldn't be able to get a record on third hiearchy if second hierarchy record doesn't exist
    intercept[StorageException] {
      exec(t => {
        val rec1 = t.from("mysql").from("table1").get("key3").from("table1_1").get("key3.4").from("table1_1_1").get()
        t.ret(rec1)
      })
    }

    val Seq(records1, records2) = exec(t => {
      val rec1 = t.from("mysql").from("table1").get("key2").from("table1_1").get()
      val rec2 = t.from("mysql").from("table1").get("key3").from("table1_1").get()
      t.ret(rec1, rec2)
    })

    assert(records1.asInstanceOf[ListValue].listValue.size == 1)
    assert(records2.asInstanceOf[ListValue].listValue.size == 3)

    val Seq(records3) = exec(t => {
      val rec1 = t.from("mysql").from("table1").get("key3").from("table1_1").get("key3.1").from("table1_1_1").get()
      t.ret(rec1)
    })

    assert(records3.asInstanceOf[ListValue].listValue.size == 2)
  }

  test("should support delete") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2"))
      table.set("key3", Map("k" -> "value3"))
      table.delete("key1")
      table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
      table.get("key3").from("table1_1").set("key3.2", Map("k" -> "value3.2"))
      table.get("key3").from("table1_1").delete("key3.1")
    }, commit = true)


    val Seq(v1, v2, v3, v4, v5) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("key2")
      val v1 = table.get("key1")
      val v2 = table.get("key2")
      val v3 = table.get("key3")
      val v4 = table.get("key3").from("table1_1").get("key3.1")
      val v5 = table.get("key3").from("table1_1").get("key3.2")
      t.ret(v1, v2, v3, v4, v5)
    }, commit = true)

    assert(v1.equalsValue(new NullValue))
    assert(v2.equalsValue(new NullValue))
    assert(!v3.equalsValue(new NullValue))
    assert(v4.equalsValue(new NullValue))
    assert(!v5.equalsValue(new NullValue))
  }

  test("deletion should be a tombstone record") {
    val transac = new MysqlTransaction(mysqlStorage, None, new MysqlTransaction.Metrics(mysqlStorage))
    val initRec = new Record(Map("test" -> 1234))
    val path = new AccessPath(Seq(new AccessKey("test1")))
    transac.set(table1, 1, createNowTimestamp(), path, Some(initRec))

    val rec1 = transac.get(table1, 1, createNowTimestamp(), path)
    assert(rec1.get.value.asInstanceOf[MapValue]("test").equalsValue(1234))

    transac.set(table1, 1, createNowTimestamp(), path, None)

    val rec2 = transac.get(table1, 1, createNowTimestamp(), path)
    assert(rec2.isEmpty)

    val rec3 = transac.get(table1, 1, createNowTimestamp(), path, includeDeleted = true)
    assert(rec3.isDefined)
    assert(rec3.get.value.isNull)

    transac.commit()
  }

  test("deleting parent should delete children") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("k1", Map("k" -> "value1@1"))
      table.get("k1").from("table1_1").set("k1.1", Map("k" -> "value1.1@1"))
      table.get("k1").from("table1_1").get("k1.1").from("table1_1_1").set("k1.1.1", Map("k" -> "value1.1.1@1"))
      table.set("k2", Map("k" -> "value1@1"))
      table.get("k2").from("table1_1").set("k2.1", Map("k" -> "value2.1@1"))
      table.delete("k2")
    })

    val Seq(rec1, rec2, rec3) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("k1")

      table.set("k1", Map("k" -> "value1@2"))
      table.set("k2", Map("k" -> "value2@2"))

      val rec1 = table.get("k1").from("table1_1").get("k1.1")
      val rec2 = table.get("k2").from("table1_1").get("k2.1")

      table.get("k1").from("table1_1").set("k1.1", Map("k" -> "value1.1@2"))
      val rec3 = table.get("k1").from("table1_1").get("k1.1").from("table1_1_1").get("k1.1.1")

      t.ret(rec1, rec2, rec3)
    })

    assert(rec1.isNull)
    assert(rec2.isNull)
    assert(rec3.isNull)

    val Seq(rec4, rec5) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      val rec4 = table.get("k1").from("table1_1").get("k1.1")
      val rec5 = table.get("k2").from("table1_1").get("k2.1")

      t.ret(rec4, rec5)
    })
    assert(rec4.asInstanceOf[MapValue]("k").equalsValue("value1.1@2"))
    assert(rec5.isNull)

  }

  test("operations should be kept in a history") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2.0"))
      table.set("key3", Map("k" -> "value3"))
      table.delete("key1")
      table.get("key3").from("table1_1").set("key3.1", Map("k" -> "value3.1"))
      table.get("key3").from("table1_1").set("key3.2", Map("k" -> "value3.2"))
      table.get("key3").from("table1_1").delete("key3.1")
      table.get("key3").from("table1_1").get("key3.2").from("table1_1_1").set("key3.2.1", Map("k" -> "value3.2.1"))
    }, commit = true, onTimestamp = createTimestamp(10))

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key4", Map("k" -> "value4"))
      table.get("key2").from("table1_1").set("key2.1", Map("k" -> "value2.1"))
      table.get("key2").from("table1_1").get("key2.1").from("table1_1_1").set("key2.1.1", Map("k" -> "value2.1.1"))
    }, commit = false, onTimestamp = createTimestamp(100))

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key5", Map("k" -> "value5"))
      table.set("key2", Map("k" -> "value2.1"))
      table.get("key2").from("table1_1").set("key2.2", Map("k" -> "value2.1"))
      table.get("key2").from("table1_1").get("key2.2").from("table1_1_1").set("key2.2.1", Map("k" -> "value2.2.1"))
      table.delete("key3")
    }, commit = true, onTimestamp = createTimestamp(200))

    val context = new ExecutionContext(storages)
    val table1Timeline = mysqlStorage.createStorageTransaction(context).getTimeline(table1, createTimestamp(0), 100)

    assert(table1Timeline.size === 6)
    assert(table1Timeline(0).accessPath.keys(0) == "key1", table1Timeline(0).accessPath.keys(0))
    assert(table1Timeline(0).newValue.isEmpty)
    assert(table1Timeline(1).accessPath.keys(0) == "key2", table1Timeline(0).accessPath.keys(0))
    table1Timeline(1).newValue match {
      case Some(m: MapValue) => assert("value2.0".equalsValue(m.mapValue("k")))
      case _ => fail()
    }
    assert(table1Timeline(2).accessPath.keys(0) == "key3", table1Timeline(0).accessPath.keys(0))
    table1Timeline(2).newValue match {
      case Some(m: MapValue) => assert("value3".equalsValue(m.mapValue("k")))
      case _ => fail()
    }
    assert(table1Timeline(3).accessPath.keys(0) == "key5", table1Timeline(0).accessPath.keys(0))
    table1Timeline(3).newValue match {
      case Some(m: MapValue) => assert("value5".equalsValue(m.mapValue("k")))
      case _ => fail()
    }
    assert(table1Timeline(4).accessPath.keys(0) == "key2", table1Timeline(0).accessPath.keys(0))
    table1Timeline(4).newValue match {
      case Some(m: MapValue) => assert("value2.1".equalsValue(m.mapValue("k")))
      case _ => fail()
    }
    table1Timeline(4).oldValue match {
      case Some(m: MapValue) => assert("value2.0".equalsValue(m.mapValue("k")))
      case _ => fail()
    }
    assert(table1Timeline(5).accessPath.keys(0) == "key3", table1Timeline(0).accessPath.keys(0))
    assert(table1Timeline(5).newValue.isEmpty)
    table1Timeline(5).oldValue match {
      case Some(m: MapValue) => assert("value3".equalsValue(m.mapValue("k")))
      case _ => fail()
    }


    val table1_1Timeline = mysqlStorage.createStorageTransaction(context).getTimeline(table1_1, createTimestamp(0), 100)
    assert(table1_1Timeline.size == 5, table1_1Timeline.size)

    val table1_1_1Timeline = mysqlStorage.createStorageTransaction(context).getTimeline(table1_1_1, createTimestamp(0), 100)
    assert(table1_1_1Timeline.size == 3, table1_1_1Timeline.size)

    val limitedTable1Timeline = mysqlStorage.createStorageTransaction(context).getTimeline(table1, createTimestamp(0), 2)
    limitedTable1Timeline.size should be(2)

    val table1TimelineAt10 = mysqlStorage.createStorageTransaction(context).getTimeline(table1, createTimestamp(10),
      0, selectMode = AtTimestamp)
    table1TimelineAt10.size should be(3)

    val table1TimelineAt100 = mysqlStorage.createStorageTransaction(context).getTimeline(table1, createTimestamp(100),
      0, selectMode = AtTimestamp)
    table1TimelineAt100.size should be(0)

    val table1TimelineAt200 = mysqlStorage.createStorageTransaction(context).getTimeline(table1, createTimestamp(200),
      0, selectMode = AtTimestamp)
    table1TimelineAt200.size should be(3)

    val token1 = context.getToken("key1")
    val token2 = context.getToken("key2")
    val ranges = List(TokenRange(token1, token1), TokenRange(token2, token2))
    val table1RangeTimeline = mysqlStorage.createStorageTransaction(context).getTimeline(table1, createTimestamp(0), 100, ranges)
    val allKeys = table1Timeline.map(_.accessPath.toString)
    val expectedRangeKeys = table1Timeline.filter(r => token1 == r.token || token2 == r.token).map(_.accessPath.toString)
    val actualRangeKeys = table1RangeTimeline.map(_.accessPath.toString)
    allKeys.size should be > (expectedRangeKeys.size)
    expectedRangeKeys.size should be >= 2
    actualRangeKeys should be(expectedRangeKeys)
  }

  test("deleted parents should generate deletion history for children") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2"))
      table.get("key1").from("table1_1").set("key1.1", Map("k" -> "value1.1"))
      table.get("key2").from("table1_1").set("key2.1", Map("k" -> "value2.1"))
      table.delete("key2")
    }, commit = true, onTimestamp = createTimestamp(10))

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("key1")
    }, commit = true, onTimestamp = createTimestamp(100))

    val context = new ExecutionContext(storages)
    val table1_1Timeline = mysqlStorage.createStorageTransaction(context).getTimeline(table1_1, createTimestamp(0), 100)
    assert(table1_1Timeline.size === 3, table1_1Timeline.size)
  }

  test("forced garbage collections should truncate versions and keep enough versions") {
    val values = mutable.Map[String, Int]()
    val rand = new Random(3234234)
    var totalCollected = 0

    for (i <- 0 to 1000) {
      val k = rand.nextInt(100).toString
      val v = values.getOrElse(k, rand.nextInt(1000))

      exec(t => {
        val storage = t.from("mysql")
        storage.from("table2").set(k, Map("k" -> v))
        storage.from("table2").get(k).from("table2_1").set(k, Map("k" -> v))
        storage.from("table2").get(k).from("table2_1").get(k).from("table2_1_1").set(k, Map("k" -> v))
      }, commit = true)

      if (rand.nextInt(10) == 5) {
        var trx = mysqlStorage.createStorageTransaction
        val beforeSizeTable2 = trx.getSize(table2)
        val beforeSizeTable2_1 = trx.getSize(table2_1)
        val beforeSizeTable2_1_1 = trx.getSize(table2_1_1)
        val totalBeforeSize = beforeSizeTable2 + beforeSizeTable2_1 + beforeSizeTable2_1_1
        trx.rollback()

        val collected = mysqlStorage.GarbageCollector.collectAll(rand.nextInt(10))
        totalCollected += collected

        trx = mysqlStorage.createStorageTransaction
        val afterSizeTable2 = trx.getSize(table2)
        val afterSizeTable2_1 = trx.getSize(table2_1)
        val afterSizeTable2_1_1 = trx.getSize(table2_1_1)
        val totalAfterSize = afterSizeTable2 + afterSizeTable2_1 + afterSizeTable2_1_1
        trx.rollback()

        assert((totalBeforeSize - collected) == totalAfterSize, "after %d > before %d, deleted %d".format(afterSizeTable2, beforeSizeTable2, collected))
      }

      values += (k -> v)
    }

    totalCollected should be > (0)

    for ((k, v) <- values) {
      val Seq(rec1, rec2, rec3) = exec(t => {
        val storage = t.from("mysql")
        val rec1 = storage.from("table2").get(k)
        val rec2 = storage.from("table2").get(k).from("table2_1").get(k)
        val rec3 = storage.from("table2").get(k).from("table2_1").get(k).from("table2_1_1").get(k)
        t.ret(rec1, rec2, rec3)
      }, commit = true)

      val val1 = rec1.asInstanceOf[MapValue].mapValue("k")
      val val2 = rec2.asInstanceOf[MapValue].mapValue("k")
      val val3 = rec2.asInstanceOf[MapValue].mapValue("k")

      assert(val1.equalsValue(v), "%s!=%s".format(rec1, v))
      assert(val2.equalsValue(v), "%s!=%s".format(rec2, v))
      assert(val3.equalsValue(v), "%s!=%s".format(rec3, v))
    }
  }

  test("force garbage collections should delete all extra versions of a token before going to the next token") {
    val context = new ExecutionContext(storages)
    val keys = 0.to(5).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    def createRecords(key: String, ver: Int) {
      println("createRecords " + key)
      for (i <- 0.until(ver)) {
        exec(t => {
          val t1 = t.from("mysql").from("table1")
          t1.set(key, Map(key -> i.toString))
        }, commit = true, onTimestamp = createTimestamp(i))
      }
    }

    createRecords(key = keys(0)._2, ver = 5)
    createRecords(key = keys(1)._2, ver = 10)
    createRecords(key = keys(2)._2, ver = 100)
    createRecords(key = keys(3)._2, ver = 500)
    createRecords(key = keys(4)._2, ver = 5)

    // Verify versions before GC
    getTokenVersion(token = keys(0)._1).get.versionsCount should be(5)
    getTokenVersion(token = keys(1)._1).get.versionsCount should be(10)
    getTokenVersion(token = keys(2)._1).get.versionsCount should be(100)
    getTokenVersion(token = keys(3)._1) match {
      // Validate that we don't get all versions in one call
      case Some(v) => {
        v.versionsCount should be(500)
        v.versions.size should be < v.versionsCount
      }
      case _ => fail("All versions loaded in one call. Need to be in multiple calls for this test to be useful.")
    }
    getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

    // Force collection once, should collect the versions of the first 3 tokens
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    getTokenVersion(token = keys(0)._1) should be(None)
    getTokenVersion(token = keys(1)._1) should be(None)
    getTokenVersion(token = keys(2)._1) should be(None)
    getTokenVersion(token = keys(3)._1).get.versionsCount should be(500)
    getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

    // Force collection again, should collect most but not all versions of the 4th token
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    getTokenVersion(token = keys(0)._1) should be(None)
    getTokenVersion(token = keys(1)._1) should be(None)
    getTokenVersion(token = keys(2)._1) should be(None)
    getTokenVersion(token = keys(3)._1).get.versionsCount should (be > 0 and be < 500)
    getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

    // This time all versions of the 4th token should have been collected
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    getTokenVersion(token = keys(0)._1) should be(None)
    getTokenVersion(token = keys(1)._1) should be(None)
    getTokenVersion(token = keys(2)._1) should be(None)
    getTokenVersion(token = keys(3)._1) should be(None)
    getTokenVersion(token = keys(4)._1).get.versionsCount should be(5)

    def getTokenVersion(token: Long): Option[VersionRecord] = {
      var trx = mysqlStorage.createStorageTransaction
      val versions = trx.getTopMostVersions(table1, token, token, 10)
      trx.rollback()

      versions.size should be <= 1
      versions.headOption
    }
  }

  test("force garbage collections should delete all extra versions of a token with multiple access path") {

    val idGenerator = new TimestampIdGenerator()
    val context = new ExecutionContext(storages)
    val k1 = "key"
    val token = context.getToken(k1)

    // Create master table record
    exec(t => {
      t.from("mysql").from("table2").set(k1, Map("k1" -> k1))
    }, commit = true, onTimestamp = createTimestamp(idGenerator.nextId))

    // Create secondary table records
    createSecondaryRecords(k2 = 0, ver = 5)
    createSecondaryRecords(k2 = 1, ver = 200)
    getSecondaryLoadedVersionCount(k2 = 1) should be < 200

    // GC first key and second key partially
    mysqlStorage.GarbageCollector.collectAll(20) should be > 0
    getSecondaryLoadedVersionCount(k2 = 0) should be(0)
    getSecondaryLoadedVersionCount(k2 = 1) should be > 0

    // Create new versions for first key and from now on limit GC to only one key at a time
    createSecondaryRecords(k2 = 0, ver = 5)
    getSecondaryLoadedVersionCount(k2 = 0) should be > 5
    mysqlStorage.GarbageCollector.collectAll(200, versionBatchSize = 1) should be > 0

    // Now GC everything remaining
    mysqlStorage.GarbageCollector.collectAll(200, versionBatchSize = 1) should be > 0
    mysqlStorage.GarbageCollector.collectAll(200, versionBatchSize = 1) should be > 0
    getSecondaryVersionCount should be(0)

    def createSecondaryRecords(k2: Int, ver: Int) {
      println("createRecords k2=%s".format(k2))
      for (i <- 0.until(ver)) {
        exec(t => {
          t.from("mysql").from("table2").get(k1).from("table2_1").set(k2.toString, Map("k1" -> k1, "k2" -> i.toString))
        }, commit = true, onTimestamp = createTimestamp(idGenerator.nextId))
      }
    }

    def getSecondaryVersionCount: Int = {
      var trx = mysqlStorage.createStorageTransaction
      val versions = trx.getTopMostVersions(table2_1, token, token, 1000)
      trx.rollback()

      versions.foldLeft(0)((count, version) => count + version.versionsCount)
    }

    def getSecondaryLoadedVersionCount(k2: Int): Int = {
      var trx = mysqlStorage.createStorageTransaction
      val versions = trx.getTopMostVersions(table2_1, token, token, 1000)
      trx.rollback()

      val path = "%s/%d".format(k1, k2)
      versions.find(_.accessPath.toString == path).map(_.versions.size).getOrElse(0)
    }
  }

  test("forced garbage collections with ranges") {
    val context = new ExecutionContext(storages)
    val keys = List.range(0, 20).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    def createRecords(seed: Long, ver: Int = 5) {
      for (((token, key), i) <- keys.zipWithIndex) {
        Seq.range(0, ver).foreach(j => {
          exec(t => {
            val t1 = t.from("mysql").from("table1")
            t1.set(key, Map(key -> "%d_%d_%d".format(seed, i, j)))
          }, commit = true, onTimestamp = createTimestamp(seed + i * ver + j))
        })
      }
    }

    val ranges = List(TokenRange(1000000001L, 2000000000L), TokenRange(3000000001L, 4000000000L))
    mysqlStorage.GarbageCollector.setCollectedRanges(ranges)

    // Create 5 versions for each token/keys tupple
    createRecords(0, 5)
    verifyRecordsCollectedForRanges(List())

    // Force collection once, should collect records of the first range
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    verifyRecordsCollectedForRanges(ranges.slice(0, 1))

    // Force collection again, should collect records of the second and last range
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    verifyRecordsCollectedForRanges(ranges)

    // Create 5 more versions for each token/keys tupple
    createRecords(5000, 5)
    verifyRecordsCollectedForRanges(List())

    // Force more collect. Collect should start over from the first range. Do a few small collects on the first range
    // and then the entire range
    mysqlStorage.GarbageCollector.collectAll(5) should be > 0
    mysqlStorage.GarbageCollector.collectAll(5) should be > 0
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    verifyRecordsCollectedForRanges(ranges.slice(0, 1))

    // Yet another collect on the second and last range
    mysqlStorage.GarbageCollector.collectAll(100) should be > 0
    verifyRecordsCollectedForRanges(ranges)

    // Final collection when there are nothing to be collected
    mysqlStorage.GarbageCollector.collectAll(100) should be(0)

    // Just for fun, try with an empty range
    // Create 5 more versions for each token/keys tupple
    createRecords(10000, 5)
    verifyRecordsCollectedForRanges(List())
    mysqlStorage.GarbageCollector.setCollectedRanges(List())
    mysqlStorage.GarbageCollector.collectAll(100) should be(0)
    verifyRecordsCollectedForRanges(List())

    def verifyRecordsCollectedForRanges(collectedRanges: List[TokenRange]) {
      var trx = mysqlStorage.createStorageTransaction
      keys.foreach(entry => {
        val (t, k) = entry
        val versions = trx.getTopMostVersions(table1, t, t, 10)

        if (collectedRanges.exists(_.contains(t))) {
          // tupple is in the collected range, no extra version should be available
          versions.size should be(0)
        } else {
          // not in collected range, some extra version should exists
          versions.size should be > 0
        }
      })
      trx.rollback()
    }
  }

  test("should be able to set the same key twice and keep the last version") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> toVal("value1")))
      table.set("key1", Map("k" -> toVal("value2")))
    }, commit = true)

    val Seq(a, b) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      val a = table.get("key1")
      table.set("key1", Map("k" -> toVal("value3")))
      val b = table.get("key1")

      t.ret(a, b)
    }, commit = false)

    a.value.serializableValue match {
      case m: MapValue => assert(m("k").equalsValue("value2"))
      case _ => fail()
    }

    b.value.serializableValue match {
      case m: MapValue => assert(m("k").equalsValue("value3"))
      case _ => fail()
    }
  }

  test("getAllLatest should return all latest elements") {

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      t1.set("key1", Map("k1" -> "v1"))
      t1.set("key2", Map("k1" -> "v1"))
      t1.set("key3", Map("k1" -> "v1"))
    }, commit = true, onTimestamp = createTimestamp(1))

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      t1.set("key2", Map("k1" -> "v2"))
      t1.delete("key3")
    }, commit = true, onTimestamp = createTimestamp(100))

    val context = new ExecutionContext(storages)
    val table1All = mysqlStorage.createStorageTransaction(context)
      .getAllLatest(table1, 100)

    assert(table1All.next() === true)
    val recordKey1 = table1All.record
    assert(recordKey1.value.asInstanceOf[MapValue]("k1").toString === "v1")

    assert(table1All.next() === true)
    val recordKey2 = table1All.record
    assert(recordKey2.value.asInstanceOf[MapValue]("k1").toString === "v2")

    assert(table1All.next() === false)

    table1All.close()
  }

  test("getAllLatest should return all latest elements on multi-hierarchical table") {
    val keys = Seq("key1", "key2", "key3")

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      keys foreach (t1.set(_, Map("k1" -> "v1")))
    }, commit = true, onTimestamp = createTimestamp(0))

    var currentTime = 100
    keys foreach (key => {
      exec(t => {
        val t11 = t.from("mysql").from("table1").get(key).from("table1_1")
        t11.set("key1", Map("k1" -> "v1"))
        t11.set("key2", Map("k1" -> "v1"))
        t11.set("key3", Map("k1" -> "v1"))
      }, commit = true, onTimestamp = createTimestamp(currentTime))

      currentTime += 10
      exec(t => {
        val t11 = t.from("mysql").from("table1").get(key).from("table1_1")
        t11.set("key2", Map("k1" -> "v2"))
        t11.delete("key3")
      }, commit = true, onTimestamp = createTimestamp(currentTime))
      currentTime += 10
    })

    exec(t => t.from("mysql").from("table1").delete("key3"), commit = true, onTimestamp = createTimestamp(currentTime))


    val context = new ExecutionContext(storages)
    val table11All = mysqlStorage.createStorageTransaction(context)
      .getAllLatest(table1_1, 100)

    assert(table11All.next() === true)
    val recordKey1 = table11All.record
    assert(recordKey1.value.asInstanceOf[MapValue]("k1").toString === "v1")

    assert(table11All.next() === true)
    val recordKey2 = table11All.record
    assert(recordKey2.value.asInstanceOf[MapValue]("k1").toString === "v2")

    assert(table11All.next() === true)
    val recordKey3 = table11All.record
    assert(recordKey3.value.asInstanceOf[MapValue]("k1").toString === "v1")

    assert(table11All.next() === true)
    val recordKey4 = table11All.record
    assert(recordKey4.value.asInstanceOf[MapValue]("k1").toString === "v2")

    assert(table11All.next() === false)

    table11All.close()

  }

  test("genWhereHigherEqualTuple should generate a where string") {

    val context = new ExecutionContext(storages)
    val transaction = mysqlStorage.createStorageTransaction(context)


    var rep = transaction.genWhereHigherEqualTuple(Map("a" -> 1))
    rep._1 should be("((a >= ?))")
    rep._2 should be(Seq(1))


    rep = transaction.genWhereHigherEqualTuple(Map("a" -> 1, "b" -> 2, "c" -> 3))
    rep._1 should be("((a > ?)) OR ((a = ?) AND (b > ?)) OR ((a = ?) AND (b = ?) AND (c >= ?))")
    rep._2 should be(Seq(1, 1, 2, 1, 2, 3))

  }

  test("getAllLatest should support fromRecord") {
    val context = new ExecutionContext(storages)
    val keys = Seq.range(0, 40).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
    }, commit = true, onTimestamp = createTimestamp(0))

    var records = mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 20).toList
    var recordsKey = records.map(_.accessPath(0).key)
    recordsKey should be(keys.map(_._2).slice(0, 20).toList)

    records = mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 20, optFromRecord = Some(records.last)).toList
    recordsKey = records.map(_.accessPath(0).key)
    recordsKey should be(keys.map(_._2).slice(19, 39).toList)
  }

  test("getAllLatest with token range") {
    val context = new ExecutionContext(storages)
    val keys = Seq.range(0, 40).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
    }, commit = true, onTimestamp = createTimestamp(0))

    val ranges = List(TokenRange(0, 1333333333L), TokenRange(1333333334L, 2666666666L),
      TokenRange(2666666667L, TokenRange.MaxToken))

    var allRecordKeys: List[String] = List()
    for (range <- ranges) {
      val records = mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 40, range).toList
      val recordsKey = records.map(_.accessPath(0).key)
      recordsKey should be(keys.filter(key => range.contains(key._1)).map(_._2).toList)
      allRecordKeys ++= recordsKey
    }

    allRecordKeys.length should be(40)
  }

  test("getAllLatest with token range and fromToken") {
    val context = new ExecutionContext(storages)
    val keys = Seq.range(0, 40).map(i => {
      val key = "key%d".format(i)
      (context.getToken(key), key)
    }).sorted

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      keys foreach (tup => t1.set(tup._2, Map(tup._2 -> tup._2)))
    }, commit = true, onTimestamp = createTimestamp(0))

    val ranges = List(TokenRange(0, 1333333333L), TokenRange(1333333334L, 2666666666L),
      TokenRange(2666666667L, TokenRange.MaxToken))

    for (range <- ranges) {
      var records = mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 40, range).toList
      var recordsKey = records.map(_.accessPath(0).key)
      recordsKey should be(keys.filter(key => range.contains(key._1)).map(_._2).toList)

      val middleIndex = records.size / 2
      records = mysqlStorage.createStorageTransaction(context).getAllLatest(table1, 40, range, Some(records(middleIndex))).toList
      recordsKey = records.map(_.accessPath(0).key)
      recordsKey should be(keys.filter(key => range.contains(key._1)).map(_._2).slice(middleIndex, 40).toList)
    }
  }

  test("calling limit on a multi-record value should offset and limit the number of records returned") {
    val keys = List.range(0, 40).map(i => {
      "key%02d".format(i)
    })

    val Seq(a, b, c, d) = exec(t => {
      val t1 = t.from("mysql").from("table1")
      t1.set("key1", Map("k" -> "v"))
      val t2 = t1.get("key1").from("table1_1")
      keys foreach (key => t2.set(key, Map(key -> key)))

      val a = t2.get()
      val b = t2.get().limit(10)
      val c = t2.get().limit(0, 10)
      val d = t2.get().limit(10, 10)

      t.returns(a, b, c, d)
    }, commit = true, onTimestamp = createTimestamp(0))

    val la = a.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)
    val lb = b.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)
    val lc = c.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)
    val ld = d.asInstanceOf[ListValue].flatMap(_.asInstanceOf[MapValue].mapValue.values).map(_.toString)

    la should be(keys)
    lb should be(keys.slice(0, 10))
    lc should be(keys.slice(0, 10))
    ld should be(keys.slice(10, 20))
  }

  test("make sure junit doesn't get stuck") {
  }
}
