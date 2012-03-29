package com.wajam.mry.storage

import com.wajam.mry.model.{Model, Table}
import org.scalatest.FunSuite
import com.wajam.mry.execution.Timestamp


/**
 * Common tests for storages
 */
trait TestStorageSuite extends FunSuite {
  def testStorage(storage: Storage) {
    val model = new Model
    val table1 = model.addTable(new Table("table1"))
    val table2 = table1.addTable(new Table("table2"))
    storage.syncModel(model)

    test("set") {
      val t = storage.getTransaction(Timestamp.now)
      t.set(table1, Seq("key1"), "value1")
      val v = t.get(table1, Seq("key1"))
      assert(v != null)
      assert(v != None)
      assert(v.get.stringValue == "value1")
      t.commit()
    }

    test("commited get") {
      val t = storage.getTransaction(Timestamp.now)
      val v = t.get(table1, Seq("key1"))
      assert(v != null)
      assert(v != None)
      assert(v.get.stringValue == "value1")
      t.set(table1, Seq("key2"), "value2")
      t.rollback()
    }

    test("uncommited get") {
      val t = storage.getTransaction(Timestamp.now)
      val v = t.get(table1, Seq("key2"))
      assert(v == None)
    }
  }
}
