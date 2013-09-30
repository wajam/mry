package com.wajam.mry.storage

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.execution.{NullValue, Transaction, ExecutionContext}
import com.wajam.commons.timestamp.Timestamp

@RunWith(classOf[JUnitRunner])
class TestMemory extends FunSuite {
  val storage = new MemoryStorage("memory")

  def createNowTimestamp() = new Timestamp {
    val value = System.currentTimeMillis() * 10000
  }

  test("commited get set") {
    var context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    var t = new Transaction()

    var store = t.from("memory")
    store.set("key1", "value1")
    var v = store.get("key1")
    t.execute(context)
    context.commit()
    assert(v.value.toString == "value1")
    assert(context.hasToken("key1"))

    t = new Transaction()
    context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    store = t.from("memory")
    v = store.get("key1")
    store.set("key2", "value2")
    t.ret(v)
    t.execute(context)
    context.rollback()
    assert(context.returnValues(0).equalsValue("value1"))
    assert(context.hasToken("key2"))
    assert(context.hasToken("key1"))
  }

  test("uncommited get set") {
    var t = new Transaction()
    val context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    val store = t.from("memory")
    store.set("key3", "value3")
    val v1 = store.get("key2")
    val v2 = store.get("key3")
    t.execute(context)
    assert(v1.value.isNull)
    assert(v2.value.equalsValue("value3"))
  }

  test("delete item shouldn't exist") {
    var context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    var t = new Transaction()

    var store = t.from("memory")
    store.set("key1", "value1")
    var v = store.get("key1")
    t.execute(context)
    context.commit()

    t = new Transaction()
    context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    store = t.from("memory")
    v = store.delete("key1")
    store.set("key2", "value2")
    store.set("key3", "value3")
    store.delete("key3")
    t.execute(context)
    context.commit()

    t = new Transaction()
    context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    store = t.from("memory")
    store.set("key4", "value4")
    store.delete("key4")
    t.execute(context)
    context.rollback()

    t = new Transaction()
    context = new ExecutionContext(Map("memory" -> storage), Some(createNowTimestamp()))
    store = t.from("memory")
    var v1 = store.get("key1")
    var v2 = store.get("key2")
    var v3 = store.get("key3")
    var v4 = store.get("key4")
    t.ret(v1, v2, v3, v4)
    t.execute(context)
    context.commit()

    assert(v1.value.isNull)
    assert(v2.value.equalsValue("value2"))
    assert(v3.value.isNull)
    assert(v4.value.isNull)
  }

}
