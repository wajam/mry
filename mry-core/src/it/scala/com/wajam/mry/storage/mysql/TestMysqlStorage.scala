package com.wajam.mry.storage.mysql

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.execution._
import com.wajam.mry.storage.Storage
import org.scalatest.{BeforeAndAfterEach, FunSuite, BeforeAndAfterAll}

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  var mysqlStorage:MysqlStorage = null
  var storages:Map[String, Storage] = null
  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table2 = table1.addTable(new Table("table2"))

  override def beforeEach() {
    mysqlStorage = new MysqlStorage("mysql", "localhost", "mry", "mry", "mry")
    storages = Map(("mysql" -> mysqlStorage))

    mysqlStorage.nuke()
    mysqlStorage.syncModel(model)
  }

  def exec(cb: (Transaction => Unit), commit: Boolean = true): Seq[Value] = {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    cb(transac)
    transac.execute(context)

    if (commit)
      context.commit()
    else
      context.rollback()

    context.returnValues
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


    val Seq(records1, records2) = exec(t => {
      val rec1 = t.from("mysql").from("table1").get("key1").from("table2").get()
      val rec2 = t.from("mysql").from("table1").get("key3").from("table2").get()
      t.ret(rec1, rec2)
    })

    assert(records1.asInstanceOf[ListValue].listValue.size == 0)
    assert(records2.asInstanceOf[ListValue].listValue.size == 2)
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

  ignore("deleting parent should delete children") {
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
    assert(table1Timeline(0).keys(0) == "key1", table1Timeline(0).keys(0))
    assert(table1Timeline(0).newValue.isEmpty)
    assert(table1Timeline(1).keys(0) == "key2", table1Timeline(0).keys(0))
    table1Timeline(1).newValue match {
      case Some(m: MapValue) => assert("value2.0".equalsValue(m.mapValue("k")))
    }
    assert(table1Timeline(2).keys(0) == "key3", table1Timeline(0).keys(0))
    table1Timeline(2).newValue match {
      case Some(m: MapValue) => assert("value3".equalsValue(m.mapValue("k")))
    }
    assert(table1Timeline(3).keys(0) == "key5", table1Timeline(0).keys(0))
    table1Timeline(3).newValue match {
      case Some(m: MapValue) => assert("value5".equalsValue(m.mapValue("k")))
    }
    assert(table1Timeline(4).keys(0) == "key2", table1Timeline(0).keys(0))
    table1Timeline(4).newValue match {
      case Some(m: MapValue) => assert("value2.1".equalsValue(m.mapValue("k")))
    }
    table1Timeline(4).oldValue match {
      case Some(m: MapValue) => assert("value2.0".equalsValue(m.mapValue("k")))
      case None => fail()
    }
    assert(table1Timeline(5).keys(0) == "key3", table1Timeline(0).keys(0))
    assert(table1Timeline(5).newValue.isEmpty)
    table1Timeline(5).oldValue match {
      case Some(m: MapValue) => assert("value3".equalsValue(m.mapValue("k")))
      case None => fail()
    }


    val table2Timeline = mysqlStorage.getStorageTransaction(context).getTimeline(table2, fromTimestamp, 100)
    assert(table2Timeline.size == 3, table2Timeline.size)

    // TODO: test second level
  }

  override protected def afterAll() {
    mysqlStorage.close()
  }
}
