package com.wajam.mry.storage.mysql.cache

import com.google.common.cache.{RemovalCause, RemovalNotification, RemovalListener, CacheBuilder}
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}
import com.wajam.commons.Logging
import com.wajam.mry.storage.mysql._

class HierarchicalCache(model: => Model, expireMs: Long, maximumSizePerTable: Int) extends CacheMetrics {
  // TODO: increment on cache invalidation (done in a separate pull request)
  lazy val invalidateCacheCounter = metrics.counter("cache-invalidation")

  // Keep one cache per top level table. All descendant tables are cached in the same cache than their top level ancestor
  private lazy val tableCaches: Map[Table, HierarchicalTableCache] = model.tables.values.map(table =>
    table -> new HierarchicalTableCache(table, this, expireMs, maximumSizePerTable)).toMap

  def start(): Unit = resetMetrics()

  def stop(): Unit = resetMetrics()

  /**
   * Returns a new transaction cache
   */
  def createTransactionCache = new TransactionCache(this, getTopLevelTableCache)

  private def getTopLevelTableCache(table: Table): HierarchicalTableCache = tableCaches(table.getTopLevelTable)
}

class HierarchicalTableCache(table: Table, metrics: CacheMetrics, expireMs: Long, maximumSize: Int) extends TableCache[Record] with Logging {

  private val keys = new ConcurrentSkipListSet[AccessPath](AccessPathOrdering)
  private val cache = CacheBuilder
    .newBuilder()
    .expireAfterAccess(expireMs, TimeUnit.MILLISECONDS)
    .maximumSize(maximumSize)
    .removalListener(CacheRemovalListener)
    .build[AccessPath, Record]

  metrics.cacheMaxSizeGauge.addTable(table, maximumSize)
  metrics.cacheCurrentSizeGauge.addTable(table, cache.size().toInt)

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