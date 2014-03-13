package com.wajam.mry.storage.mysql.cache

import language.implicitConversions
import org.scalatest.Matchers._
import org.scalatest.FlatSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

@RunWith(classOf[JUnitRunner])
class TestHierarchicalTableCache extends FlatSpec {

  "HierarchicalTableCache" should "cache all values" in new CacheSetup {
    val cache = new HierarchicalTableCache(table1, new CacheMetrics {}, 1000, 200)

    // Cache all records and verify their presence/value
    all.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Update one leaf value and verify cache is updated
    cache.getIfPresent(b_a_a.accessPath) should be(Some(b_a_a))
    val b_a_a2 = record(b_a_a.table, b_a_a.accessPath, "2")
    cache.put(b_a_a2.accessPath, b_a_a2)
    cache.getIfPresent(b_a_a.accessPath) should be(Some(b_a_a2))
  }

  it should "update should NOT invalidate children" in new CacheSetup {
    val cache = new HierarchicalTableCache(table1, new CacheMetrics {}, 1000, 200)

    // Cache all records and verify their presence/value
    all.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Update one value and verify cache is updated and children NOT invalidated
    cache.getIfPresent(a.accessPath) should be(Some(a))
    val a2 = record(a.table, a.accessPath, "2")
    cache.put(a2.accessPath, a2)
    cache.getIfPresent(a.accessPath) should be(Some(a2))
    cache.getIfPresent(a_a.accessPath) should be(Some(a_a))
    all_a.tail.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}
    all_but_a.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}
  }

  it should "invalidate should invalidate children" in new CacheSetup {
    val cache = new HierarchicalTableCache(table1, new CacheMetrics {}, 1000, 200)

    // Cache all records and verify their presence/value
    all.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Invalidate a leaf and verify not cached anymore
    cache.getIfPresent(c.accessPath) should be(Some(c))
    cache.invalidate(c.accessPath)
    cache.getIfPresent(c.accessPath) should be(None)
    val all_not_c = withoutAncestor(all, "c")
    all_not_c.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}

    // Invalidate a parent and verify itself/children not cached anymore
    cache.getIfPresent(a.accessPath) should be(Some(a))
    cache.invalidate(a.accessPath)
    all_a.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    val all_not_c_and_a = withoutAncestor(all_not_c, "a")
    all_not_c_and_a.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}
  }

  it should "evict least recently used record when reaching max cache size" in new CacheSetup {
    val cache = new HierarchicalTableCache(table1, new CacheMetrics {}, 1000, maximumSize = all_a.size)

    all_a.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    all_a.foreach(r => cache.put(r.accessPath, r))
    all_a.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}
    all_but_a.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}

    // Add an extra record, the first record should be evicted. Note that even if the evicted record has children
    // in the cache, the children are not evicted.
    cache.put(c.accessPath, c)
    cache.getIfPresent(c.accessPath) should be(Some(c))
    cache.getIfPresent(all_a.head.accessPath) should be(None)
    all_a.head should be(a)

    val other_a = all_a.filterNot(_.accessPath == all_a.head.accessPath)
    other_a.foreach { r => cache.getIfPresent(r.accessPath) should be(Some(r))}
  }

  it should "evict records after expiration" in new CacheSetup with Eventually {
    val cache = new HierarchicalTableCache(table1, new CacheMetrics {}, expireMs = 25, 200)
    implicit override val patienceConfig =
      PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(30, Millis)))

    all.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    all.foreach(r => cache.put(r.accessPath, r))
    all.exists(r => cache.getIfPresent(r.accessPath) == Some(r))

    eventually {
      all.foreach { r => cache.getIfPresent(r.accessPath) should be(None)}
    }
  }
}
