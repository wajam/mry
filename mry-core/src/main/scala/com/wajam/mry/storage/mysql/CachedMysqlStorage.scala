package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.ExecutionContext

trait CachedMysqlStorage extends MysqlStorage {

  private val cache = new HierarchicalCache(this)

  override def createStorageTransaction(context: ExecutionContext) = new MysqlTransaction(this, Some(context)) with CachedMysqlTransaction {
    val cacheTransaction: HierarchicalCache#CacheTransaction = cache.getTransaction
  }

  override def createStorageTransaction = new MysqlTransaction(this, None) with CachedMysqlTransaction {
    val cacheTransaction: HierarchicalCache#CacheTransaction = cache.getTransaction
  }
}
