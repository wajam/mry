package com.appaquet.mry.storage

import org.scalatest.FunSuite
import com.appaquet.mry.model.{Table, Model}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestMemoryStorage extends FunSuite {
  val storage = new MemoryStorage

  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  storage.syncModel(model)

  test("set") {
    val t = storage.getTransaction
    t.set(table1, Seq("key1"), "value1")
    val v = t.get(table1, Seq("key1"))
    assert(v != None)
    assert(v.get.stringValue == "value1")
    t.commit()
  }

  test("commited get") {
    val t = storage.getTransaction
    val v = t.get(table1, Seq("key1"))
    assert(v != None)
    assert(v.get.stringValue == "value1")
    t.set(table1, Seq("key2"), "value2")
    t.rollback()
  }

  test("uncommited get") {
    val t = storage.getTransaction
    val v = t.get(table1, Seq("key2"))
    assert(v == None)
  }
}
