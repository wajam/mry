package com.wajam.mry.storage.mysql

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import com.wajam.mry.execution.{NullValue, ExecutionContext, Transaction}

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends FunSuite with BeforeAndAfterAll {
  val storage = new MysqlStorage("mysql", "localhost", "mry", "mry", "mry")
  val storages = Map(("mysql" -> storage))
  storage.nuke()

  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table2 = table1.addTable(new Table("table2"))
  storage.syncModel(model)

  test("set") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table = storage.from("table1")
    table.set("value1", "key1")
    val v = table.get("key1")
    transac.execute(context)
    context.commit()
    assert(v.value.serializableValue.equalsValue("value1"))
  }

  test("commited get") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table = storage.from("table1")
    val v = table.get("key1")
    table.set("value2", "key2")
    transac.execute(context)
    assert(v.value.serializableValue.equalsValue("value1"))
  }

  test("uncommited get") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table = storage.from("table1")
    val v = table.get("key2")
    transac.execute(context)
    assert(v.value.equalsValue(new NullValue), "got %s".format(v.value))
  }

  test("multi level get/set") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table1 = storage.from("table1")
    val record1 = table1.get("key1")

    val table2 = record1.from("table2")
    table2.set("value1", "key1.2")

    val record2 = table2.get("key1.2")

    transac.execute(context)
    context.commit()
    assert(record2.value.equalsValue("value1"), "got %s".format(record2.value))
  }

  /*
  test("multi level get/set") {
    var t = storage.getStorageTransaction(Timestamp.now)
    t.set(table2, Seq("key1", "key2"), "value3")
    var v = t.get(table2, Seq("key1", "key2"))
    assert(v != null)
    assert(v != None)
    assert(v.get.stringValue == "value3")
    t.commit()

    t = storage.getStorageTransaction(Timestamp.now)
    v = t.get(table2, Seq("key1", "key2"))
    assert(v != null)
    assert(v != None)
    assert(v.get.stringValue == "value3")
  }
  */

  override protected def afterAll() {
    storage.close()
  }
}
