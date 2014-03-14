package com.wajam.mry.storage.mysql.cache

import com.wajam.mry.storage.mysql.{AccessPath, Record, Table}
import com.wajam.commons.Logging
import scala.annotation.tailrec

class TransactionCache(metrics: CacheMetrics, getTableCache: (Table) => TableCache[Record]) extends Logging {

  import TransactionCache._

  // Keep one cache per top level table. All descendant tables are cached in the same cache than their top level ancestor
  private var trxTableCaches: Map[Table, TransactionTableCache] = Map.empty

  /**
   * Returns the specified table path cached record or load the record with the provided function. If loaded, the
   * record is cached in the current transaction cache.
   */
  def getOrSet(table: Table, path: AccessPath, record: => Option[Record]): Option[Record] = {
    get(table, path) match {
      case Some(CachedValue(rec, _, _)) => {
        metrics.hitsMeter(table).foreach(_.mark())
        rec
      }
      case None => {
        val rec = record
        metrics.missesMeter(table).foreach(_.mark())
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
        case (path, CachedValue(Some(rec), _, invalidateDescendants)) if invalidateDescendants => {
          // Support resurrected access path i.e. an access path deleted earlier in the transaction.
          // The original descendants are invalidated before transferring the new cached record to the storage
          // cache.
          getTableCache(table).invalidate(path)
          getTableCache(table).put(path, rec)
        }
        case (path, CachedValue(Some(rec), _, _)) => getTableCache(table).put(path, rec)
        case (path, CachedValue(None, _, _)) => getTableCache(table).invalidate(path)
      }
    }
    trxTableCaches = Map.empty
  }

  /**
   * Returns the specified table path cached record. Use the record value cached in the current transaction but
   * fallback to the storage cache if allowed (i.e. ancestor not deleted) and necessary.
   */
  private[cache] def get(table: Table, path: AccessPath): Option[CachedValue] = {
    val trxTableCache = getOrCreateTrxCache(table)
    trxTableCache.getIfPresent(path) match {
      case rec@Some(_) => rec
      case None if trxTableCache.isAncestorInvalidated(path) => None
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

  private def getOrCreateTrxCache(table: Table): TransactionTableCache = {
    val topLevelTable = table.getTopLevelTable

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
      trace(s"put(): $path=$value")

      if (value.action == Action.Put && value.record.isEmpty) {
        // If the cached record is deleted, invalidate its descendants before caching it
        getDescendants(path).foreach { case (descPath, _) => invalidate(descPath)}
        cache.put(path, value.copy(invalidateDescendants = true))
      } else {
        Option(cache.put(path, value)) match {
          case Some(prevValue) if prevValue.invalidateDescendants => {
            // Support resurrected access path i.e. caching a record for an access path deleted earlier in the
            // transaction. The original descendants must be invalidated when transferring the value to the storage
            // cache..
            cache.put(path, value.copy(invalidateDescendants = true))
          }
          case _ =>
        }
      }
    }

    def invalidate(path: AccessPath) = cache.remove(path)

    def toIterable: Iterable[(AccessPath, CachedValue)] = {
      import collection.JavaConversions._
      cache.entrySet().toIterator.toIterable.map(e => e.getKey -> e.getValue)
    }

    @tailrec
    final def isAncestorInvalidated(path: AccessPath): Boolean = {
      val parentPathSeq = path.parts.dropRight(1)
      if (parentPathSeq.isEmpty) {
        false
      } else {
        val parentPath = AccessPath(parentPathSeq)
        getIfPresent(parentPath) match {
          case Some(CachedValue(_, _, invalidateDescendants)) if invalidateDescendants => true
          case _ => isAncestorInvalidated(parentPath)
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

  case class CachedValue(record: Option[Record], action: Action, invalidateDescendants: Boolean = false)

}
