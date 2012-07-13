package com.wajam.mry.storage.mysql

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import com.wajam.mry.storage.{StorageException, Storage}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import collection.mutable
import util.Random

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends FunSuite with BeforeAndAfterEach {
  var mysqlStorage: MysqlStorage = null
  var storages: Map[String, Storage] = null
  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table2 = table1.addTable(new Table("table2"))

  override def beforeEach() {
    this.mysqlStorage = newStorageInstance()
  }

  def newStorageInstance() = {
    val storage = new MysqlStorage("mysql", "localhost", "mry", "mry", "mry", garbageCollection = false)
    storages = Map(("mysql" -> storage))

    storage.nuke()
    storage.syncModel(model)
    storage.start()

    storage
  }

  override protected def afterEach() {
    mysqlStorage.stop()
  }

  def exec(cb: (Transaction => Unit), commit: Boolean = true): Seq[Value] = {
    val context = new ExecutionContext(storages)

    try {
      val transac = new Transaction()
      cb(transac)
      transac.execute(context)

      context.returnValues
    } finally {
      if (commit)
        context.commit()
      else
        context.rollback()
    }
  }

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
    val Seq(record1, record2) = exec(t => {
      val storage = t.from("mysql")
      val table1 = storage.from("table1")
      table1.set("key1", Map("k1" -> "value1"))

      val record1 = table1.get("key1")
      val table2 = record1.from("table2")
      table2.set("key1.2", Map("mapk" -> toVal("value1")))

      val record2 = table2.get("key1.2")

      t.ret(record1, record2)
    }, commit = true)

    record2.value.serializableValue match {
      case m: MapValue =>
        assert(m("mapk").equalsValue("value1"))
    }
  }

  test("shouldn't be able to get multiple rows on first depth table") {
    intercept[InvalidParameter] {
      val Seq(records) = exec(t => {
        t.ret(t.from("mysql").from("table1").get())
      })
    }
  }

  test("should be able to get multiple rows on second depth table") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2"))
      table.set("key3", Map("k" -> "value3"))
      table.delete("key1")
      table.get("key3").from("table2").set("key3.1", Map("k" -> "value3.1"))
      table.get("key3").from("table2").set("key3.2", Map("k" -> "value3.2"))
    }, commit = true)

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.get("key2").from("table2").set("key2.1", Map("k" -> "value2.1"))
      table.get("key3").from("table2").set("key3.3", Map("k" -> "value3.3"))
    }, commit = true)


    // shouldn't be able to get a record on second hiearchy if first hierarchy record doesn't exist
    intercept[StorageException] {
      exec(t => {
        val rec1 = t.from("mysql").from("table1").get("key1").from("table2").get()
        t.ret(rec1)
      })
    }

    val Seq(records1, records2) = exec(t => {
      val rec1 = t.from("mysql").from("table1").get("key2").from("table2").get()
      val rec2 = t.from("mysql").from("table1").get("key3").from("table2").get()
      t.ret(rec1, rec2)
    })

    assert(records1.asInstanceOf[ListValue].listValue.size == 1)
    assert(records2.asInstanceOf[ListValue].listValue.size == 3)
  }

  test("should support delete") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2"))
      table.set("key3", Map("k" -> "value3"))
      table.delete("key1")
      table.get("key3").from("table2").set("key3.1", Map("k" -> "value3.1"))
      table.get("key3").from("table2").set("key3.2", Map("k" -> "value3.2"))
      table.get("key3").from("table2").delete("key3.1")
    }, commit = true)


    val Seq(v1, v2, v3, v4, v5) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("key2")
      val v1 = table.get("key1")
      val v2 = table.get("key2")
      val v3 = table.get("key3")
      val v4 = table.get("key3").from("table2").get("key3.1")
      val v5 = table.get("key3").from("table2").get("key3.2")
      t.ret(v1, v2, v3, v4, v5)
    }, commit = true)

    assert(v1.equalsValue(new NullValue))
    assert(v2.equalsValue(new NullValue))
    assert(!v3.equalsValue(new NullValue))
    assert(v4.equalsValue(new NullValue))
    assert(!v5.equalsValue(new NullValue))
  }

  test("deletion should be a tombstone record") {
    val transac = new MysqlTransaction(mysqlStorage)
    val initRec = new Record(Map("test" -> 1234))
    val path = new AccessPath(Seq(new AccessKey("test1", Some(1))))
    transac.set(table1, 1, Timestamp.now, path, Some(initRec))

    val rec1 = transac.get(table1, 1, Timestamp.now, path)
    assert(rec1.get.value.asInstanceOf[MapValue]("test").equalsValue(1234))

    transac.set(table1, 1, Timestamp.now, path, None)

    val rec2 = transac.get(table1, 1, Timestamp.now, path)
    assert(rec2.isEmpty)

    val rec3 = transac.get(table1, 1, Timestamp.now, path, includeDeleted = true)
    assert(rec3.isDefined)
    assert(rec3.get.value.isNull)

    transac.commit()
  }

  test("deleting parent should delete children") {
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("k1", Map("k" -> "value1@1"))
      table.get("k1").from("table2").set("k1.1", Map("k" -> "value1.1@1"))
      table.set("k2", Map("k" -> "value1@1"))
      table.get("k2").from("table2").set("k2.1", Map("k" -> "value2.1@1"))
      table.delete("k2")
    })

    val Seq(rec1, rec2) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.delete("k1")

      table.set("k1", Map("k" -> "value1@2"))
      table.set("k2", Map("k" -> "value2@2"))

      val rec1 = table.get("k1").from("table2").get("k1.1")
      val rec2 = table.get("k2").from("table2").get("k2.1")

      t.ret(rec1, rec2)
    })

    val Seq(rec3, rec4) = exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      val rec1 = table.get("k1").from("table2").get("k1.1")
      val rec2 = table.get("k2").from("table2").get("k2.1")

      t.ret(rec1, rec2)
    })
    assert(rec3.isNull)
    assert(rec4.isNull)

  }

  test("operations should be kept in a history") {
    Thread.sleep(100)
    val fromTimestamp = Timestamp.now
    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key1", Map("k" -> "value1"))
      table.set("key2", Map("k" -> "value2.0"))
      table.set("key3", Map("k" -> "value3"))
      table.delete("key1")
      table.get("key3").from("table2").set("key3.1", Map("k" -> "value3.1"))
      table.get("key3").from("table2").set("key3.2", Map("k" -> "value3.2"))
      table.get("key3").from("table2").delete("key3.1")
    }, commit = true)

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key4", Map("k" -> "value4"))
      table.get("key2").from("table2").set("key2.1", Map("k" -> "value2.1"))
    }, commit = false)

    exec(t => {
      val storage = t.from("mysql")
      val table = storage.from("table1")
      table.set("key5", Map("k" -> "value5"))
      table.set("key2", Map("k" -> "value2.1"))
      table.get("key2").from("table2").set("key2.2", Map("k" -> "value2.1"))
      table.delete("key3")
    }, commit = true)

    val context = new ExecutionContext(storages)
    val table1Timeline = mysqlStorage.getStorageTransaction(context).getTimeline(table1, fromTimestamp, 100)

    assert(table1Timeline.size == 6)
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


    val table2Timeline = mysqlStorage.getStorageTransaction(context).getTimeline(table2, fromTimestamp, 100)
    assert(table2Timeline.size == 3, table2Timeline.size)

    // TODO: test second level
  }

  test("forced garbage collections should truncate versions and keep enough versions") {
    val values = mutable.Map[String, Int]()
    val rand = new Random(3234234)

    for (i <- 0 to 2000) {
      val k = rand.nextInt(100).toString
      var v = values.getOrElse(k, rand.nextInt(1000))

      exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        table.set(k, Map("k" -> v))
      }, commit = true)

      if (rand.nextInt(10) == 5) {
        var trx = mysqlStorage.getStorageTransaction
        val beforeSize = trx.getSize(table1)
        trx.rollback()

        val collected = mysqlStorage.GarbageCollector.collect(rand.nextInt(10))

        trx = mysqlStorage.getStorageTransaction
        val afterSize = trx.getSize(table1)
        trx.rollback()

        assert((beforeSize - collected) == afterSize, "after %d > before %d, deleted %d".format(afterSize, beforeSize, collected))
      }

      values += (k -> v)
    }

    for ((k, v) <- values) {
      val Seq(rec1) = exec(t => {
        val storage = t.from("mysql")
        val table = storage.from("table1")
        t.ret(table.get(k))
      }, commit = true)

      val curVal = rec1.asInstanceOf[MapValue].mapValue("k")
      assert(curVal.equalsValue(v), "%s!=%s".format(rec1, v))
    }
  }

  test("make sure junit doesn't get stuck") {
  }
}
