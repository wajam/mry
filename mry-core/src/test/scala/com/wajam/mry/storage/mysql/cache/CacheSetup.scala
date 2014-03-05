package com.wajam.mry.storage.mysql.cache

import language.implicitConversions
import com.wajam.mry.storage.mysql._
import org.scalatest.Matchers._
import com.wajam.mry.storage.mysql.AccessPath
import com.wajam.mry.storage.mysql.AccessKey
import com.wajam.mry.execution.StringValue

trait CacheSetup {
  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table1_1 = table1.addTable(new Table("table1_1"))
  val table1_1_1 = table1_1.addTable(new Table("table1_1_1"))
  val table2 = model.addTable(new Table("table2"))
  val table2_1 = table2.addTable(new Table("table2_1"))
  val table2_1_1 = table2_1.addTable(new Table("table2_1_1"))

  val a = record(table1, "a", "a")
  val a_a = record(table1_1, ("a", "a"), "a_a")
  val a_b = record(table1_1, ("a", "b"), "a_b")
  val a_bb = record(table1_1, ("a", "bb"), "a_bb")
  val aa_a = record(table1_1, ("aa", "a"), "aa_a")
  val b = record(table1, "b", "b")
  val bb = record(table1, "bb", "bb")
  val b_a = record(table1_1, ("b", "a"), "b_a")
  val b_a_a = record(table1_1_1, ("b", "a", "a"), "b_a_a")
  val c = record(table1, "c", "c")

  val all = Seq(a, a_a, a_b, a_bb, aa_a, b, b_a, b_a_a, bb, c)
  val all_a = withAncestor(all, "a")
  val all_but_a = withoutAncestor(all, "a")

  all_a.size should be(4)
  all_but_a.size should be(all.size - all_a.size)

  def path(keys: String*) = AccessPath(keys.toList.map(new AccessKey(_)))

  def record(table: Table, path: AccessPath, value: String): Record = {
    new Record(table, StringValue(value), 0L, path) {
      // TODO: explain why override!
      override def equals(that: Any) = that match {
        case that: Record => that.value.equals(this.value) && this.accessPath.keys.equals(that.accessPath.keys)
        case _ => false
      }
    }
  }

  def withAncestor(records: Seq[Record], ancestor: String) = {
    records.filter(r => r.accessPath.parts.head.key == ancestor)
  }

  def withoutAncestor(records: Seq[Record], ancestor: String) = {
    records.filter(r => r.accessPath.parts.head.key != ancestor)
  }

  implicit def seqToPath(keys: Seq[String]): AccessPath = AccessPath(keys.toList.map(new AccessKey(_)))

  implicit def tuple1ToPath(t: (String)): AccessPath = path(t)

  implicit def tuple2ToPath(t: (String, String)): AccessPath = path(t._1, t._2)

  implicit def tuple3ToPath(t: (String, String, String)): AccessPath = path(t._1, t._2, t._3)
}


