package com.wajam.mry.storage.mysql

import org.scalatest.Matchers._
import org.scalatest.FlatSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.wajam.mry.execution.StringValue
import language.implicitConversions
import scala.util.Random
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

@RunWith(classOf[JUnitRunner])
class TestHierarchicalTableCache extends FlatSpec {

  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table1_1 = table1.addTable(new Table("table1_1"))
  val table1_1_1 = table1_1.addTable(new Table("table1_1_1"))
  val table2 = model.addTable(new Table("table2"))
  val table2_1 = table2.addTable(new Table("table2_1"))
  val table2_1_1 = table2_1.addTable(new Table("table2_1_1"))

  implicit def tuple1ToList[T](t: (T)): List[T] = List(t)
  implicit def tuple2ToList[T](t: (T,T)): List[T] = List(t._1, t._2)
  implicit def tuple3ToList[T](t: (T,T,T)): List[T] = List(t._1, t._2, t._3)

  implicit def seqToPath(keys: Seq[String]): AccessPath = AccessPath(keys.toList.map(new AccessKey(_)))
  implicit def tuple1ToPath(t: (String)): AccessPath = List(t)
  implicit def tuple2ToPath(t: (String, String)): AccessPath = List(t._1, t._2)
  implicit def tuple3ToPath(t: (String, String, String)): AccessPath = List(t._1, t._2, t._3)

  def toPath(keys: String*) = AccessPath(keys.toList.map(new AccessKey(_)))

  def toRecord(table: Table, path: AccessPath, value: String): Record = {
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

  // TODO: convert to trait
  object Records {
    val a = toRecord(table1, "a", "a")
    val a_a = toRecord(table1_1, ("a", "a"), "a_a")
    val a_b = toRecord(table1_1, ("a", "b"), "a_b")
    val a_bb = toRecord(table1_1, ("a", "bb"), "a_bb")
    val aa_a = toRecord(table1_1, ("aa", "a"), "aa_a")
    val b = toRecord(table1, "b", "b")
    val bb = toRecord(table1, "bb", "bb")
    val b_a = toRecord(table1_1, ("b", "a"), "b_a")
    val b_a_a = toRecord(table1_1_1, ("b", "a", "a"), "b_a_a")
    val c = toRecord(table1, "c", "c")

    val all = Seq(a, a_a, a_b, a_bb, aa_a, b, b_a, b_a_a, bb, c)
//    val all_shuffled = Random.shuffle(all)
    val all_a = withAncestor(all, "a")
    val all_aa = withAncestor(all, "aa")
    val all_b = withAncestor(all, "b")
    val all_bb = withAncestor(all, "bb")
    val all_c = withAncestor(all, "c")
    val all_not_a = withoutAncestor(all, "a")
//    val all_not_c = withoutAncestor(all, "c")

    all_a.size should be(4)
    all_aa.size should be(1)
    all_b.size should be(3)
    all_bb.size should be(1)
    all_c.size should be(1)
    all_not_a.size should be(all.size-all_a.size)
  }

  "HierarchicalTableCache" should "cache all values" in {
    import Records._

    val cache = new HierarchicalTableCache(1000, 200)

    // Cache all records and verify their presence/value
    all.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Update one leaf value and verify cache is updated
    cache.getIfPresent(b_a_a.accessPath) should be(Some(b_a_a))
    val b_a_a2 = toRecord(b_a_a.table, b_a_a.accessPath, "2")
    cache.put(b_a_a2.accessPath, b_a_a2)
    cache.getIfPresent(b_a_a.accessPath) should be(Some(b_a_a2))
  }

  it should "update should invalidate children" in {
    import Records._

    val cache = new HierarchicalTableCache(1000, 200)

    // Cache all records and verify their presence/value
    all.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Update one value and verify cache is updated and children invalidated
    cache.getIfPresent(a.accessPath) should be(Some(a))
    val a2 = toRecord(a.table, a.accessPath, "2")
    cache.put(a2.accessPath, a2)
    cache.getIfPresent(a.accessPath) should be(Some(a2))
    cache.getIfPresent(a_a.accessPath) should be(None)
    all_a.foreach{r => cache.getIfPresent(r.accessPath) should not be Some(r)}
    all_not_a.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}
  }

  it should "invalidate should invalidate children" in {
    import Records._

    val cache = new HierarchicalTableCache(1000, 200)

    // Cache all records and verify their presence/value
    all.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Invalidate a leaf and verify not cached anymore
    cache.getIfPresent(c.accessPath) should be(Some(c))
    cache.invalidate(c.accessPath)
    cache.getIfPresent(c.accessPath) should be(None)
    val all_not_c = withoutAncestor(all, "c")
    all_not_c.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Invalidate a parent and verify itself/children not cached anymore
    cache.getIfPresent(a.accessPath) should be(Some(a))
    cache.invalidate(a.accessPath)
    all_a.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    val all_not_c_and_a = withoutAncestor(all_not_c, "a")
    all_not_c_and_a.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}
  }

  it should "evict least recently used record when reaching max cache size" in {
    import Records._

    val cache = new HierarchicalTableCache(1000, maximumSize = all_a.size)

    all_a.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    all_a.foreach(r => cache.put(r.accessPath, r))
    all_a.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}
    all_not_a.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}

    // Add an extra record, the first record should be evicted. Note that even if the evicted record has children
    // in the cache, the children are not evicted.
    cache.put(c.accessPath, c)
    cache.getIfPresent(c.accessPath) should be(Some(c))
    cache.getIfPresent(all_a.head.accessPath) should be(None)
    all_a.head should be(a)

    val other_a = all_a.filterNot(_.accessPath == all_a.head.accessPath)
    other_a.foreach{r => cache.getIfPresent(r.accessPath) should be(Some(r))}
  }

  it should "evict records after expiration" in new Eventually {
    import Records._

    implicit override val patienceConfig =
      PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(30, Millis)))

    val cache = new HierarchicalTableCache(expireMs = 25, 200)

    all.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.exists(r => cache.getIfPresent(r.accessPath) == Some(r))

    eventually {
      all.foreach{r => cache.getIfPresent(r.accessPath) should be(None)}
    }
  }


  "AccessPathOrdering" should "compare" in {
    import Records._

    AccessPathOrdering.compare(a_a.accessPath, aa_a.accessPath) should be(-1)
    AccessPathOrdering.compare(aa_a.accessPath, a_a.accessPath) should be(1)
    AccessPathOrdering.compare(a.accessPath, toPath("a")) should be(0)
  }

  it should "sort" in {
    import Records._

    val sortedPaths = Random.shuffle(all.map(_.accessPath)).sorted(AccessPathOrdering)
    val expectedPaths = all.map(_.accessPath)
    sortedPaths should be(expectedPaths)
  }

  it should "verify ancestor" in {
    import Records._

    AccessPathOrdering.isAncestor(a.accessPath, a.accessPath) === false
    AccessPathOrdering.isAncestor(a.accessPath, a_a.accessPath) === true
    AccessPathOrdering.isAncestor(a.accessPath, b.accessPath) === false
    AccessPathOrdering.isAncestor(b.accessPath, bb.accessPath) === false
    AccessPathOrdering.isAncestor(b_a.accessPath, b.accessPath) === false
    AccessPathOrdering.isAncestor(b_a.accessPath, b_a_a.accessPath) === false
    AccessPathOrdering.isAncestor(b.accessPath, b_a_a.accessPath) === true
  }

  "HierarchicalCache" should "cache records per trx and flush them in the global cache when trx is committed" in {
    import Records._

    val cache = new HierarchicalCache(model, 1000, 200)
    val a2 = toRecord(a.table, a.accessPath, "2")

    // Empty cache + new trx cache. Both should be empty. Load records in trx cache without committing trx.
    // The other trx should not see the loaded values.
    val trxCache1 = cache.createTransactionCache
    all.foreach{r => trxCache1.get(r.table, r.accessPath) should be(None)}
    all.foreach{r => trxCache1.getOrSet(r.table, r.accessPath, Some(r)) should be(Some(r))}
    all.foreach{r => trxCache1.get(r.table, r.accessPath) should be(Some(Some(r)))}

    // New trx cache. Both are still empty. Load and commit records. Future trx should see the loaded values.
    val trxCache2 = cache.createTransactionCache
    all.foreach{r => trxCache2.get(r.table, r.accessPath) should be(None)}
    all.foreach{r => trxCache2.getOrSet(r.table, r.accessPath, Some(r)) should be(Some(r))}
    trxCache2.commit()

    // New trx now see the cached values
    val trxCache3 = cache.createTransactionCache
    all.foreach{r => trxCache3.get(r.table, r.accessPath) should be(Some(Some(r)))}

    // New trx update one record without committing.
    val trxCache4 = cache.createTransactionCache
    trxCache4.put(a.table, a.accessPath, Some(a2))
    trxCache4.get(a.table, a.accessPath) should be(Some(Some(a2)))

    // New trx does not see the value updated but not committed by previous trx
    val trxCache5 = cache.createTransactionCache
    trxCache5.get(a.table, a.accessPath) should be(Some(Some(a)))

    // New trx update the record and commit it this time
    val trxCache6 = cache.createTransactionCache
    trxCache6.put(a.table, a.accessPath, Some(a2))
    trxCache6.commit()

    // New trx see the updated value
    val trxCache7 = cache.createTransactionCache
    trxCache7.get(a.table, a.accessPath) should be(Some(Some(a2)))

    // New trx reset the value commit it
    val trxCache8 = cache.createTransactionCache
    trxCache8.put(a.table, a.accessPath, None)
    trxCache8.commit()

    // New trx see the reset value
    val trxCache9 = cache.createTransactionCache
    trxCache9.get(a.table, a.accessPath) should be(None)
  }

  it should "cache values per table" in {
    import Records._

    val t1_a = toRecord(table1, a.accessPath, "t1_a")
    val t2_a = toRecord(table2, a.accessPath, "t2_a")

    val cache = new HierarchicalCache(model, 1000, 200)

    val trxCache1 = cache.createTransactionCache
    trxCache1.get(t1_a.table, a.accessPath) should be(None)
    trxCache1.get(t2_a.table, a.accessPath) should be(None)
    trxCache1.getOrSet(t1_a.table, a.accessPath, Some(t1_a))
    trxCache1.get(t1_a.table, a.accessPath) should be(Some(Some(t1_a)))
    trxCache1.get(t2_a.table, a.accessPath) should be(None)
    trxCache1.commit()

    val trxCache2 = cache.createTransactionCache
    trxCache2.get(t1_a.table, a.accessPath) should be(Some(Some(t1_a)))
    trxCache2.get(t2_a.table, a.accessPath) should be(None)
    trxCache2.put(t2_a.table, a.accessPath, Some(t2_a))
    trxCache2.get(t1_a.table, a.accessPath) should be(Some(Some(t1_a)))
    trxCache2.get(t2_a.table, a.accessPath) should be(Some(Some(t2_a)))
    trxCache2.commit()

//    val trxCache3 = cache.createTransactionCache
//    trxCache3.get(t1_a.table, a.accessPath) should be(Some(Some(t1_a)))
//    trxCache3.get(t2_a.table, a.accessPath) should be(Some(Some(t2_a)))
//    trxCache3.put(t2_a.table, a.accessPath, None)
//    trxCache3.get(t1_a.table, a.accessPath) should be(Some(Some(t1_a)))
//    trxCache3.get(t2_a.table, a.accessPath) should be(None)
//    trxCache3.commit()
//
//    val trxCache4 = cache.createTransactionCache
//    trxCache4.get(t1_a.table, a.accessPath) should be(Some(Some(t1_a)))
//    trxCache4.get(t2_a.table, a.accessPath) should be(None)
  }

  ignore should "cache hierarchical values" in {
    import Records._

    val cache = new HierarchicalCache(model, 1000, 200)
    val a2 = toRecord(a.table, a.accessPath, "2")

    val trxCache1 = cache.createTransactionCache
    trxCache1.get(a.table, a.accessPath) should be(None)
    trxCache1.get(a_a.table, a_a.accessPath) should be(None)
    trxCache1.get(a_b.table, a_b.accessPath) should be(None)
    trxCache1.put(a_b.table, a_b.accessPath, Some(a_b))
    trxCache1.put(a.table, a_b.accessPath, Some(a))
    trxCache1.get(a.table, a.accessPath) should be(Some(a))
    trxCache1.get(a_a.table, a_a.accessPath) should be(None)
    trxCache1.get(a_b.table, a_b.accessPath) should be(Some(a_b))
    trxCache1.commit()

    val trxCache2 = cache.createTransactionCache
    trxCache2.get(a.table, a.accessPath) should be(Some(a))
    trxCache2.get(a_a.table, a_a.accessPath) should be(None)
    trxCache2.get(a_b.table, a_b.accessPath) should be(Some(a_b))
    trxCache2.put(a.table, a_b.accessPath, Some(a2))
    trxCache2.get(a.table, a.accessPath) should be(Some(a2))
    trxCache2.get(a_a.table, a_a.accessPath) should be(None)
    trxCache2.get(a_b.table, a_b.accessPath) should be(None)
    trxCache2.commit()

    val trxCache3 = cache.createTransactionCache
    trxCache3.get(a.table, a.accessPath) should be(Some(a2))
    trxCache3.get(a_a.table, a_a.accessPath) should be(None)
    trxCache3.get(a_b.table, a_b.accessPath) should be(None)
  }
}
