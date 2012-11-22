package com.wajam.mry.execution

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.wajam.mry.storage.MemoryStorage
import com.wajam.mry.execution.Implicits._
import com.wajam.scn.Timestamp

@RunWith(classOf[JUnitRunner])
class TestTransaction extends FunSuite {
  val storage = new MemoryStorage("memory")

  test("token") {
    var context = new ExecutionContext(Map("memory" -> storage), Some(Timestamp.now))
    context.dryMode = true

    new Transaction(b => {
      val store = b.from("memory")
      b.ret(store.get("test"))
    }).execute(context)

    assert(context.hasToken("test"))
  }
}
