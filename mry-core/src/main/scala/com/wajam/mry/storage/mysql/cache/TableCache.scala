package com.wajam.mry.storage.mysql.cache

import com.wajam.mry.storage.mysql.{AccessKey, AccessPath}
import scala.annotation.tailrec

/**
 * Trait to manipulate cached records per table
 */
trait TableCache[V] {
  def getIfPresent(key: AccessPath): Option[V]

  def put(key: AccessPath, record: V): Unit

  def invalidate(key: AccessPath): Unit
}

trait ResettableTableCache[V] extends TableCache[V] {
  def invalidateAll(): Unit
}