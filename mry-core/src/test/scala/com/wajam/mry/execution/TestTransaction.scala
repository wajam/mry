package com.wajam.mry.execution

import org.scalatest.FunSuite
import com.wajam.mry.execution.Implicits._
import com.wajam.mry.storage.MemoryStorage
import com.wajam.mry.model.{Table, Model}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTransaction extends FunSuite {
  val storage = new MemoryStorage
  val model = new Model
  model.addTable(new Table("table1"))

  test("execute") {
    var context = new ExecutionContext(model, storage)
    var t = new Transaction()
    var table = t.from("table1")
    table.set("key1", "value1")
    t.execute(context)
    context.storageTransaction.commit()

    context = new ExecutionContext(model, storage)
    t = new Transaction()
    table = t.from("table1")
    val v = table.get("key1")
    t.ret(v)
    t.execute(context)

    assert(context.returnValues(0).toString == "value1")
  }
}
