package com.wajam.mry.storage.mysql

import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit

class HierarchicalCache(private val storage: MysqlStorage) {
  val maximumSize = 1000
  val expireMinutes = 10

  private lazy val cache = storage.model.allHierarchyTables.map(table =>
    table -> CacheBuilder
      .newBuilder()
      .expireAfterAccess(expireMinutes, TimeUnit.MINUTES)
      .maximumSize(maximumSize)
      .build[AccessPath, Record]
  ).toMap

  def getTransaction = new CacheTransaction

  class CacheTransaction {
    val trxCache = storage.model.allHierarchyTables.map(table =>
      table -> scala.collection.mutable.HashMap[AccessPath, Option[Record]]()
    ).toMap

    def get(table: Table, path: AccessPath): Option[Option[Record]] = {
      if (path.parts.size == 1)
        trxCache(table).get(path) match {
          case Some(rec) =>
            Some(rec)

          case None =>
            Option(cache(table).getIfPresent(path)) match {
              case Some(rec) =>
                trxCache(table) += (path -> Some(rec))
                Some(Some(rec))
              case None =>
                None
            }
        }
      else
        None
    }

    def getOrSet(table: Table, path: AccessPath, f: => Option[Record]) = get(table, path) match {
      case Some(rec) =>
        rec

      case None =>
        val rec = f
        put(table, path, rec)
        rec
    }

    def put(table: Table, path: AccessPath, record: =>Option[Record]) {
      if (path.parts.size == 1)
        trxCache(table) += (path -> record)
      else
        record
    }

    def commit() {
      trxCache.foreach {
        case (table, trxTableCache) =>
          trxTableCache.foreach {
            case (path, Some(rec)) =>
              cache(table).put(path, rec)
            case (path, None) =>
              cache(table).invalidate(path)
          }
      }
    }
  }

}
