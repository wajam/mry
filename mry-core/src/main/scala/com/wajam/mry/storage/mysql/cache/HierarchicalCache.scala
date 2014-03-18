package com.wajam.mry.storage.mysql.cache

import com.google.common.cache.{RemovalCause, RemovalNotification, RemovalListener, CacheBuilder}
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}
import com.wajam.commons.Logging
import com.wajam.mry.storage.mysql._

class HierarchicalCache(model: => Model, expireMs: Long, maximumSizePerTable: Int) extends CacheMetrics {
  lazy val invalidateCacheCounter = metrics.counter("cache-invalidation")

  // Keep one cache per top level table. All descendant tables are cached in the same cache than their top level ancestor
  private lazy val tableCaches: Map[Table, ResettableHierarchicalTableCache] = model.tables.values.map(table =>
    table -> new ResettableHierarchicalTableCache(table, this, expireMs, maximumSizePerTable)).toMap

  def start(): Unit = resetMetrics()

  def stop(): Unit = resetMetrics()

  /**
   * Returns a new transaction cache
   */
  def createTransactionCache = {
    // Make a copy of the current hierarchical table caches at the beginning of the transaction.
    // If the cache is invalidated during the transaction, the result of the transaction will NOT be flushed in
    // the new reset caches but flush in the old unused copies instead.
    val caches = tableCaches.map { case (table, cache) => table -> cache.innerCache }
    new TransactionCache(this, (table) => caches(table.getTopLevelTable))
  }

  def invalidateAll(): Unit = {
    invalidateCacheCounter += 1
    tableCaches.valuesIterator.foreach(_.invalidateAll())
  }
}

/**
 * This class can be invoked from multiple threads for different accessPath but only one thread per accessPath at
 * a time. The NRV messaging Switchboard provide this guarantee.
 */
class HierarchicalTableCache(table: Table, metrics: CacheMetrics, expireMs: Long, maximumSize: Int)
  extends TableCache[Record] with Logging {

  private val keys = new ConcurrentSkipListSet[AccessPath](AccessPathOrdering)
  private val cache = CacheBuilder
    .newBuilder()
    .expireAfterAccess(expireMs, TimeUnit.MILLISECONDS)
    .maximumSize(maximumSize)
    .removalListener(CacheRemovalListener)
    .build[AccessPath, Record]

  def getIfPresent(path: AccessPath): Option[Record] = {
    Option(cache.getIfPresent(path))
  }

  def put(path: AccessPath, record: Record) = {
    trace(s"put(): $path, $record")
    keys.add(path)
    cache.put(path, record)
  }

  def invalidate(path: AccessPath) = {
    trace(s"invalidate(): $path")
    invalidateDescendants(path)
    keys.remove(path)
    cache.invalidate(path)
  }

  def size = cache.size()

  private def invalidateDescendants(path: AccessPath): Unit = {
    import collection.JavaConversions._
    import AccessPathOrdering.isAncestor

    trace(s"invalidateDescendants(): $path")
    keys.tailSet(path, false).takeWhile(isAncestor(path, _)).foreach { child =>
      trace(s" removing: $child")
      keys.remove(child)
      cache.invalidate(child)
    }
  }

  private object CacheRemovalListener extends RemovalListener[AccessPath, Record] {
    def onRemoval(notification: RemovalNotification[AccessPath, Record]): Unit = {
      val cause = notification.getCause
      trace(s" onRemoval(): $cause, evicted=${wasEvicted(cause)}, $notification")
      cause match {
        case RemovalCause.SIZE => {
          metrics.evictionSizeCounter(table).foreach(_ += 1)
          keys.remove(notification.getKey)
        }
        case RemovalCause.EXPIRED => {
          metrics.evictionExpiredCounter(table).foreach(_ += 1)
          keys.remove(notification.getKey)
        }
        case _ =>
      }
    }

    private def wasEvicted(cause: RemovalCause) = cause != RemovalCause.EXPLICIT && cause != RemovalCause.REPLACED
  }

}

/**
 * A resettable wrapper around HierarchicalTableCache which provide an atomic implementation of invalidateAll()
 * i.e. put() and invalidateAll() can be invoked concurrently by different threads and the result still be consistent.
 */
class ResettableHierarchicalTableCache(table: Table, metrics: CacheMetrics, expireMs: Long, maximumSize: Int)
  extends ResettableTableCache[Record] with Logging {

  @volatile
  private var cache = new HierarchicalTableCache(table, metrics, expireMs, maximumSize)

  metrics.cacheMaxSizeGauge.addTable(table, maximumSize)
  metrics.cacheCurrentSizeGauge.addTable(table, cache.size.toInt)

  def getIfPresent(path: AccessPath) = cache.getIfPresent(path)

  def put(path: AccessPath, record: Record) = cache.put(path, record)

  def invalidate(path: AccessPath) = cache.invalidate(path)

  def invalidateAll(): Unit = {
    trace(s"invalidateAll(): $table")
    cache = new HierarchicalTableCache(table, metrics, expireMs, maximumSize)
  }

  private[cache] def innerCache = cache
}