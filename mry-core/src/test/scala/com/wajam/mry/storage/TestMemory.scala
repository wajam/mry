package com.wajam.mry.storage

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.execution.{NullValue, Transaction, ExecutionContext}

@RunWith(classOf[JUnitRunner])
class TestMemory extends FunSuite {
  val storage = new MemoryStorage("memory")

  test("commited get set") {
    var context = new ExecutionContext(Map("memory" -> storage))
    var t = new Transaction()

    var store = t.from("memory")
    store.set("key1", "value1")
    var v = store.get("key1")
    t.execute(context)
    context.commit()
    assert(v.value.toString == "value1")
    assert(context.hasToken("key1"))

    t = new Transaction()
    context = new ExecutionContext(Map("memory" -> storage))
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
    val context = new ExecutionContext(Map("memory" -> storage))
    val store = t.from("memory")
    store.set("key3", "value3")
    val v1 = store.get("key2")
    val v2 = store.get("key3")
    t.execute(context)
    assert(v1.value.isInstanceOf[NullValue])
    assert(v2.value.equalsValue("value3"))
  }
}
