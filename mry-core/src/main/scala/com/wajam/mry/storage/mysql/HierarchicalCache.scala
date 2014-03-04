package com.wajam.mry.storage.mysql

import com.google.common.cache.{RemovalCause, RemovalNotification, RemovalListener, CacheBuilder}
import java.util.concurrent.{ConcurrentSkipListSet, TimeUnit}
import scala.annotation.tailrec
import com.wajam.commons.Logging

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

/**
 * Trait to manipulate cached records per table
 */
trait TableCache[V] {
  def getIfPresent(key: AccessPath): Option[V]

  def put(key: AccessPath, record: V): Unit

  def invalidate(key: AccessPath): Unit
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

class TransactionCache(getTableCache: (Table) => TableCache[Record]) extends Logging {

  import TransactionCache._

  private var trxTableCaches: Map[Table, TransactionTableCache] = Map.empty

  /**
   * Returns the specified table path cached record. Use the record value cached in the current transaction but
   * fallback to the storage cache if necessary.
   */
  private[mysql] def get(table: Table, path: AccessPath): Option[CachedValue] = {
    val trxTableCache = getOrCreateTrxCache(table)
    trxTableCache.getIfPresent(path) match {
      case rec@Some(_) => rec
      case None if trxTableCache.isAncestorUpdated(path) => None
      case None => {
        getTableCache(table).getIfPresent(path) match {
          case rec@Some(_) => {
            val value = CachedValue(rec, Action.Get)
            trxTableCache.put(path, value)
            Some(value)
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
      case Some(CachedValue(rec, _)) => rec
      case None => {
        val rec = record
        getOrCreateTrxCache(table).put(path, CachedValue(rec, Action.Get))
        rec
      }
    }
  }

  /**
   * Update the cached table path record
   */
  def put(table: Table, path: AccessPath, record: => Option[Record]): Unit = {
    getOrCreateTrxCache(table).put(path, CachedValue(record, Action.Put))
  }

  /**
   * Flush the current transaction cache to the storage cache.
   */
  def commit(): Unit = {
    implicit val ordering = AccessPathOrdering

    trxTableCaches.foreach {
      case (table, trxTableCache) => trxTableCache.toIterable.foreach {
        case (path, CachedValue(Some(rec), _)) => getTableCache(table).put(path, rec)
        case (path, CachedValue(None, _)) => getTableCache(table).invalidate(path)
      }
    }
    trxTableCaches = Map.empty
  }

  private def getOrCreateTrxCache(table: Table): TransactionTableCache = {
    val topLevelTable = table.getTopLevelTable()

    trxTableCaches.get(topLevelTable) match {
      case Some(cache) => cache
      case None => {
        val cache = new TransactionTableCache()
        trxTableCaches += topLevelTable -> cache
        cache
      }
    }
  }

  private class TransactionTableCache extends TableCache[CachedValue] {
    private var cache = new java.util.TreeMap[AccessPath, CachedValue](AccessPathOrdering)

    def getIfPresent(path: AccessPath) = Option(cache.get(path))

    def put(path: AccessPath, value: CachedValue) = {
      info(s"put(): $path=$value")

      if (value.action == Action.Put && value.record.isEmpty) {
        // If the cached record is deleted, invalidate its descendants
        getDescendants(path).foreach { case (descPath, _) => invalidate(descPath)}
      }
      cache.put(path, value)
    }

    def invalidate(path: AccessPath) = cache.remove(path)

    def toIterable: Iterable[(AccessPath, CachedValue)] = {
      import collection.JavaConversions._

      info(s"toIterable()")
      cache.entrySet().toIterator.toIterable.map(e => e.getKey -> e.getValue)
    }

    @tailrec
    final def isAncestorUpdated(path: AccessPath): Boolean = {
      val parentPathSeq = path.parts.dropRight(1)
      if (parentPathSeq.isEmpty) {
        false
      } else {
        val parentPath = AccessPath(parentPathSeq)
        getIfPresent(parentPath) match {
          case Some(CachedValue(_, Action.Put)) => true
          case _ => isAncestorUpdated(parentPath)
        }
      }
    }

    private def getDescendants(path: AccessPath): Iterable[(AccessPath, CachedValue)] = {
      import collection.JavaConversions._
      import AccessPathOrdering.isAncestor

      cache.tailMap(path, false).takeWhile(t => isAncestor(path, t._1))
    }
  }

}

object TransactionCache {

  sealed trait Action

  object Action {

    object Get extends Action {
      override def toString = "Get"
    }

    object Put extends Action {
      override def toString = "Put"
    }

  }

  case class CachedValue(record: Option[Record], action: Action)

}
