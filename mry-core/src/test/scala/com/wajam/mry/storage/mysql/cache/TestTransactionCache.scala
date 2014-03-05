package com.wajam.mry.storage.mysql.cache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import TransactionCache._

@RunWith(classOf[JUnitRunner])
class TestTransactionCache extends FlatSpec {

  "TransactionCache" should "cache records per trx and flush them in the global cache when trx is committed" in new CacheSetup {
    val cache = new HierarchicalCache(model, 1000, 200)
    val a2 = record(a.table, a.accessPath, "2")

    // Empty cache + new trx cache. Both should be empty. Load records in trx cache without committing trx.
    // The other trx should not see the loaded values.
    val trxCache1 = cache.createTransactionCache
    all.foreach { r => trxCache1.get(r.table, r.accessPath) should be(None)}
    all.foreach { r => trxCache1.getOrSet(r.table, r.accessPath, Some(r)) should be(Some(r))}
    all.foreach { r => trxCache1.get(r.table, r.accessPath) should be(Some(CachedValue(Some(r), Action.Get)))}

    // New trx cache. Both are still empty. Load and commit records. Future trx should see the loaded values.
    val trxCache2 = cache.createTransactionCache
    all.foreach { r => trxCache2.get(r.table, r.accessPath) should be(None)}
    all.foreach { r => trxCache2.getOrSet(r.table, r.accessPath, Some(r)) should be(Some(r))}
    trxCache2.commit()

    // New trx now see the cached values
    val trxCache3 = cache.createTransactionCache
    all.foreach { r => trxCache3.get(r.table, r.accessPath) should be(Some(CachedValue(Some(r), Action.Get)))}

    // New trx update one record without committing.
    val trxCache4 = cache.createTransactionCache
    trxCache4.put(a.table, a.accessPath, Some(a2))
    trxCache4.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a2), Action.Put)))

    // New trx does not see the value updated but not committed by previous trx
    val trxCache5 = cache.createTransactionCache
    trxCache5.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a), Action.Get)))

    // New trx update the record and commit it this time
    val trxCache6 = cache.createTransactionCache
    trxCache6.put(a.table, a.accessPath, Some(a2))
    trxCache6.commit()

    // New trx see the updated value
    val trxCache7 = cache.createTransactionCache
    trxCache7.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a2), Action.Get)))

    // New trx reset the value commit it
    val trxCache8 = cache.createTransactionCache
    trxCache8.put(a.table, a.accessPath, None)
    trxCache8.commit()

    // New trx see the reset value
    val trxCache9 = cache.createTransactionCache
    trxCache9.get(a.table, a.accessPath) should be(None)
  }

  it should "cache values per table" in new CacheSetup {
    val t1_a = record(table1, a.accessPath, "t1_a")
    val t2_a = record(table2, a.accessPath, "t2_a")

    val cache = new HierarchicalCache(model, 1000, 200)

    val trxCache1 = cache.createTransactionCache
    trxCache1.get(t1_a.table, a.accessPath) should be(None)
    trxCache1.get(t2_a.table, a.accessPath) should be(None)
    trxCache1.getOrSet(t1_a.table, a.accessPath, Some(t1_a))
    trxCache1.get(t1_a.table, a.accessPath) should be(Some(CachedValue(Some(t1_a), Action.Get)))
    trxCache1.get(t2_a.table, a.accessPath) should be(None)
    trxCache1.commit()

    val trxCache2 = cache.createTransactionCache
    trxCache2.get(t1_a.table, a.accessPath) should be(Some(CachedValue(Some(t1_a), Action.Get)))
    trxCache2.get(t2_a.table, a.accessPath) should be(None)
    trxCache2.put(t2_a.table, a.accessPath, Some(t2_a))
    trxCache2.get(t1_a.table, a.accessPath) should be(Some(CachedValue(Some(t1_a), Action.Get)))
    trxCache2.get(t2_a.table, a.accessPath) should be(Some(CachedValue(Some(t2_a), Action.Put)))
    trxCache2.commit()

    val trxCache3 = cache.createTransactionCache
    trxCache3.get(t1_a.table, a.accessPath) should be(Some(CachedValue(Some(t1_a), Action.Get)))
    trxCache3.get(t2_a.table, a.accessPath) should be(Some(CachedValue(Some(t2_a), Action.Get)))
  }

  it should "be invalidated when trx cache is deleted (i.e. record == None)" in new CacheSetup {
    val cache = new HierarchicalCache(model, 1000, 200)

    val trxCache1 = cache.createTransactionCache
    trxCache1.get(a.table, a.accessPath) should be(None)
    trxCache1.getOrSet(a.table, a.accessPath, Some(a))
    trxCache1.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a), Action.Get)))
    trxCache1.commit()

    val trxCache2 = cache.createTransactionCache
    trxCache2.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a), Action.Get)))
    trxCache2.put(a.table, a.accessPath, None)
    trxCache2.get(a.table, a.accessPath) should be(Some(CachedValue(None, Action.Put)))
    trxCache2.commit()

    val trxCache3 = cache.createTransactionCache
    trxCache3.get(a.table, a.accessPath) should be(None)
  }

  it should "cache hierarchical values" in new CacheSetup {
    val cache = new HierarchicalCache(model, 1000, 200)
    val a2 = record(a.table, a.accessPath, "2")

    val trxCache1 = cache.createTransactionCache
    trxCache1.get(a.table, a.accessPath) should be(None)
    trxCache1.get(a_a.table, a_a.accessPath) should be(None)
    trxCache1.get(a_b.table, a_b.accessPath) should be(None)
    trxCache1.getOrSet(a.table, a.accessPath, Some(a))
    trxCache1.getOrSet(a_b.table, a_b.accessPath, Some(a_b))
    trxCache1.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a), Action.Get)))
    trxCache1.get(a_a.table, a_a.accessPath) should be(None)
    trxCache1.get(a_b.table, a_b.accessPath) should be(Some(CachedValue(Some(a_b), Action.Get)))
    trxCache1.commit()

    val trxCache2 = cache.createTransactionCache
    trxCache2.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a), Action.Get)))
    trxCache2.get(a_a.table, a_a.accessPath) should be(None)
    trxCache2.get(a_b.table, a_b.accessPath) should be(Some(CachedValue(Some(a_b), Action.Get)))

    // Updating parent record should NOT invalidate the descendants
    val trxCache3 = cache.createTransactionCache
    trxCache3.put(a.table, a.accessPath, Some(a2))
    trxCache3.commit()

    val trxCache4 = cache.createTransactionCache
    trxCache4.get(a.table, a.accessPath) should be(Some(CachedValue(Some(a2), Action.Get)))
    trxCache4.get(a_a.table, a_a.accessPath) should be(None)
    trxCache4.get(a_b.table, a_b.accessPath) should be(Some(CachedValue(Some(a_b), Action.Get)))

    // Deleting parent record should invalidate the descendants
    val trxCache5 = cache.createTransactionCache
    trxCache5.put(a.table, a.accessPath, None)
    trxCache5.commit()

    val trxCache6 = cache.createTransactionCache
    trxCache6.get(a.table, a.accessPath) should be(None)
    trxCache6.get(a_a.table, a_a.accessPath) should be(None)
    trxCache6.get(a_b.table, a_b.accessPath) should be(None)
  }
}
