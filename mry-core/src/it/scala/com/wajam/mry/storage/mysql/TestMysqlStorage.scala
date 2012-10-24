package com.wajam.mry.storage.mysql

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import com.wajam.mry.storage.{StorageException, Storage}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import collection.mutable
import util.Random
import com.wajam.scn.storage.TimestampUtil
import com.wajam.scn.Timestamp

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends FunSuite with BeforeAndAfterEach {
  var mysqlStorage: MysqlStorage = null
  var storages: Map[String, Storage] = null
  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table1_1 = table1.addTable(new Table("table1_1"))
  val table1_1_1 = table1_1.addTable(new Table("table1_1_1"))
  val table2 = model.addTable(new Table("table2"))
  val table2_1 = table2.addTable(new Table("table2_1"))
  val table2_1_1 = table2_1.addTable(new Table("table2_1_1"))

  override def beforeEach() {
    this.mysqlStorage = newStorageInstance()
  }

  def newStorageInstance() = {
    val storage = new MysqlStorage(MysqlStorageConfiguration("mysql", "localhost", "mry", "mry", "mry"), garbageCollection = false)
    storages = Map(("mysql" -> storage))

    storage.nuke()
    storage.syncModel(model)
    storage.start()

    storage
  }

  override protected def afterEach() {
    mysqlStorage.stop()
  }

  def exec(cb: (Transaction => Unit), commit: Boolean = true, onTimestamp: Timestamp = TimestampUtil.now): Seq[Value] = {
    val context = new ExecutionContext(storages, Some(onTimestamp))

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
    val transac = new MysqlTransaction(mysqlStorage, None)
    val initRec = new Record(Map("test" -> 1234))
    val path = new AccessPath(Seq(new AccessKey("test1")))
    transac.set(table1, 1, TimestampUtil.now, path, Some(initRec))

    val rec1 = transac.get(table1, 1, TimestampUtil.now, path)
    assert(rec1.get.value.asInstanceOf[MapValue]("test").equalsValue(1234))

    transac.set(table1, 1, TimestampUtil.now, path, None)

    val rec2 = transac.get(table1, 1, TimestampUtil.now, path)
    assert(rec2.isEmpty)

    val rec3 = transac.get(table1, 1, TimestampUtil.now, path, includeDeleted = true)
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
    val fromTimestamp = TimestampUtil.now
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

        val collected = mysqlStorage.GarbageCollector.collect(rand.nextInt(10))

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

  test("getAllLatest should return all latest elements") {

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      t1.set("key1", Map("k1" -> "v1"))
      t1.set("key2", Map("k1" -> "v1"))
      t1.set("key3", Map("k1" -> "v1"))
    }, commit = true, onTimestamp = createTimestamp(0))

    exec(t => {
      val t1 = t.from("mysql").from("table1")
      t1.set("key2", Map("k1" -> "v2"))
      t1.delete("key3")
    }, commit = true, onTimestamp = createTimestamp(100))

    val context = new ExecutionContext(storages)
    val table1All = mysqlStorage.createStorageTransaction(context)
      .getAllLatest(table1, createTimestamp(0), createTimestamp(200), 100)

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
      keys foreach(t1.set(_, Map("k1" -> "v1")))
    }, commit = true, onTimestamp = createTimestamp(0) )

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
      .getAllLatest(table1_1, createTimestamp(0), createTimestamp(400), 100)

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

  test("make sure junit doesn't get stuck") {
  }

  def createTimestamp(time: Long) = new Timestamp {
    def value = time
  }
}
