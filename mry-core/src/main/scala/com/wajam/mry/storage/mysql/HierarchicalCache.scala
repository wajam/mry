package com.wajam.mry.storage.mysql

import com.google.common.cache.{RemovalCause, RemovalNotification, RemovalListener, CacheBuilder}
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}
import scala.annotation.tailrec
import com.wajam.commons.Logging

class HierarchicalCache(model: => Model, expireMs: Long, maximumSizePerTable: Int) {

  // Keep one cache per top level table. All descendant tables are cached in the same cache than their ancestor
  private lazy val tableCaches: Map[Table, HierarchicalTableCache] = model.tables.values.map(table =>
    table -> new HierarchicalTableCache(expireMs, maximumSizePerTable)).toMap

  /**
   * Returns a new transaction cache
   */
  def createTransactionCache = new TransactionCache(getTopLevelTableCache)

  private def getTopLevelTableCache(table: Table): HierarchicalTableCache = tableCaches(getTopLevelTable(table))

  @tailrec
  private def getTopLevelTable(table: Table): Table = {
    table.parentTable match {
      case Some(parent) => getTopLevelTable(parent)
      case None => table
    }
  }
}

/**
 * Trait to manipulate cached records per table
 */
trait TableCache[K, V] {
  def getIfPresent(key: K): Option[V]

  def put(key: K, record: V): Unit

  def invalidate(key: K): Unit
}


class HierarchicalTableCache(expireMs: Long, maximumSize: Int) extends TableCache[AccessPath, Record] with Logging {

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
    invalidateChildren(record.accessPath)
    keys.add(record.accessPath)
    cache.put(record.accessPath, record)
  }

  def invalidate(path: AccessPath) = {
    info(s"invalidate(): $path")
    invalidateChildren(path)
    keys.remove(path)
    cache.invalidate(path)
  }

  private def invalidateChildren(path: AccessPath): Unit = {
    import collection.JavaConversions._
    import AccessPathOrdering.isAncestor

    info(s"invalidateChildren(): $path")
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

object AccessPathOrdering extends Ordering[AccessPath] {
  def compare(path1: AccessPath, path2: AccessPath) = {
    compare(path1.parts, path2.parts)
  }

  def isAncestor(ancestor: AccessPath, child: AccessPath): Boolean = {
    ancestor.length < child.length && ancestor.parts == child.parts.take(ancestor.length)
  }

  @tailrec
  private def compare(keys1: Seq[AccessKey], keys2: Seq[AccessKey]): Int = {
    (keys1, keys2) match {
      case (Nil, Nil) => 0
      case (Nil, _) => -1
      case (_, Nil) => 1
      case (h1 :: t1, h2 :: t2) => {
        val result = h1.key.compareTo(h2.key)
        if (result == 0) compare(t1, t2) else result
      }
    }
  }
}

class TransactionCache(getTableCache: (Table) => TableCache[AccessPath, Record]) {
  private var trxTableCaches: Map[Table, TransactionTableCache] = Map.empty

  // TODO explain Option[Option[Record]]
  /**
   * Returns the specified table path cached record. Use the record value cached in the current transaction but
   * fallback to the storage cache if necessary.
   */
  def get(table: Table, path: AccessPath): Option[Option[Record]] = {
    getOrCreateTrxCache(table).getIfPresent(path) match {
      case rec@Some(_) => rec
      case None => {
        getTableCache(table).getIfPresent(path) match {
          case rec@Some(_) => {
            getOrCreateTrxCache(table).put(path, rec)
            Some(rec)
          }
          case None => None
        }
      }
    }
  }

  /**
   * Returns the specified table path cached record or load the record with the provided function. If loaded, the
   * record is cached in the current transaction cache.
   */
  def getOrSet(table: Table, path: AccessPath, record: => Option[Record]): Option[Record] = {
    get(table, path) match {
      case Some(rec) => rec
      case None => {
        val rec = record
        put(table, path, rec)
        rec
      }
    }
  }

  /**
   * Update the cached table path record
   */
  def put(table: Table, path: AccessPath, record: => Option[Record]): Unit = {
    getOrCreateTrxCache(table).put(path, record)
  }

  /**
   * Flush the current transaction cache to the storage cache.
   */
  def commit(): Unit = {

    implicit val ordering = AccessPathOrdering

    trxTableCaches.foreach {
      case (table, trxTableCache) => trxTableCache.toIterable.toSeq.sortBy(_._1).foreach {
        case (path, Some(rec)) => getTableCache(table).put(path, rec)
        case (path, None) => getTableCache(table).invalidate(path)
      }
    }
    trxTableCaches = Map.empty
  }

  private def getOrCreateTrxCache(table: Table): TransactionTableCache = {
    trxTableCaches.get(table) match {
      case Some(cache) => cache
      case None => {
        val cache = new TransactionTableCache()
        trxTableCaches += table -> cache
        cache
      }
    }
  }

  private class TransactionTableCache extends TableCache[AccessPath, Option[Record]] {
    private var cache = Map[AccessPath, Option[Record]]()

    def getIfPresent(path: AccessPath) = cache.get(path)

    def put(path: AccessPath, record: Option[Record]) = cache += path -> record

    def invalidate(path: AccessPath) = cache -= path

    def toIterable: Iterable[(AccessPath, Option[Record])] = cache.toIterable
  }
}
