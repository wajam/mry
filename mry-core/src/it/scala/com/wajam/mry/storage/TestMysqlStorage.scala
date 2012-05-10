package com.wajam.mry.storage

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.execution.Implicits._
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import com.wajam.mry.execution._

/**
 * Test MySQL storage
 */
@RunWith(classOf[JUnitRunner])
class TestMysqlStorage extends FunSuite with BeforeAndAfterAll {
  val storage = new MysqlStorage("mysql", "localhost", "mry", "mry", "mry")
  val storages = Map(("mysql" -> storage))
  storage.nuke()

  val model = new MysqlModel
  val table1 = model.addTable(new MysqlTable("table1"))
  val table2 = table1.addTable(new MysqlTable("table2"))
  storage.syncModel(model)

  test("set") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table = storage.from("table1")
    table.set("key1", Map("mapk" -> toVal("value1")))
    val v = table.get("key1")
    transac.ret(v)
    transac.execute(context)
    context.commit()

    v.value.serializableValue match {
      case m: MapValue =>
        assert(m("mapk").equalsValue("value1"))
      case _ =>
        fail("Didn't receive a map")
    }
  }

  test("commited get") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table = storage.from("table1")
    val v = table.get("key1")
    table.set("key2", Map("mapk" -> toVal("value2")))
    transac.ret(v)
    transac.execute(context)

    v.value.serializableValue match {
      case m: MapValue =>
        assert(m("mapk").equalsValue("value1"))
      case _ =>
        fail("Didn't receive a map")
    }
    context.rollback()
  }

  test("uncommited get") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table = storage.from("table1")
    val v = table.get("key2")
    transac.ret(v)
    transac.execute(context)
    assert(v.value.serializableValue.equalsValue(new NullValue))
  }

  test("multi level get/set") {
    val transac = new Transaction()
    val context = new ExecutionContext(storages)
    val storage = transac.from("mysql")
    val table1 = storage.from("table1")
    val record1 = table1.get("key1")

    val table2 = record1.from("table2")
    table2.set("key1.2", Map("mapk" -> toVal("value1")))

    val record2 = table2.get("key1.2")

    transac.ret(record1, record2)
    transac.execute(context)
    context.commit()

    record2.value.serializableValue match {
      case m: MapValue =>
        assert(m("mapk").equalsValue("value1"))
      case _ =>
        fail("Didn't receive a map")
    }
  }

  override protected def afterAll() {
    storage.close()
  }
}
