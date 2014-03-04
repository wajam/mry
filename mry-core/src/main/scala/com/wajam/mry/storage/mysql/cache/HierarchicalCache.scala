package com.wajam.mry.storage.mysql.cache

import com.google.common.cache.{RemovalCause, RemovalNotification, RemovalListener, CacheBuilder}
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}
import com.wajam.commons.Logging
import com.wajam.mry.storage.mysql._
import com.wajam.mry.storage.mysql.AccessPath

class HierarchicalCache(model: => Model, expireMs: Long, maximumSizePerTable: Int) {

  // Keep one cache per top level table. All descendant tables are cached in the same cache than their top level ancestor
  private lazy val tableCaches: Map[Table, HierarchicalTableCache] = model.tables.values.map(table =>
    table -> new HierarchicalTableCache(expireMs, maximumSizePerTable)).toMap

  /**
   * Returns a new transaction cache
   */
  def createTransactionCache = new TransactionCache(getTopLevelTableCache)

  private def getTopLevelTableCache(table: Table): HierarchicalTableCache = tableCaches(table.getTopLevelTable())
}

class HierarchicalTableCache(expireMs: Long, maximumSize: Int) extends TableCache[Record] with Logging {

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
    info(s"put(): $record")
    keys.add(record.accessPath)
    cache.put(record.accessPath, record)
  }

  def invalidate(path: AccessPath) = {
    info(s"invalidate(): $path")
    invalidateDescendants(path)
    keys.remove(path)
    cache.invalidate(path)
  }

  private def invalidateDescendants(path: AccessPath): Unit = {
    import collection.JavaConversions._
    import AccessPathOrdering.isAncestor

    info(s"invalidateDescendants(): $path")
    keys.tailSet(path, false).takeWhile(isAncestor(path, _)).foreach { child =>
      info(s" removing: $child")
      keys.remove(child)
      cache.invalidate(child)
    }
  }

  private object CacheRemovalListener extends RemovalListener[AccessPath, Record] {
    def onRemoval(notification: RemovalNotification[AccessPath, Record]) {
      val cause = notification.getCause
      info(s" onRemoval(): ${notification.getCause}, evicted=${wasEvicted(cause)}, $notification")
      if (wasEvicted(cause)) {
        keys.remove(notification.getKey)
      }
    }

    private def wasEvicted(cause: RemovalCause) = cause != RemovalCause.EXPLICIT && cause != RemovalCause.REPLACED
  }

}