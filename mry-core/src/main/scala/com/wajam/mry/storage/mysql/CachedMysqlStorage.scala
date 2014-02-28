package com.wajam.mry.storage.mysql

import com.wajam.mry.execution.ExecutionContext

trait CachedMysqlStorage extends MysqlStorage {

  // TODO: make this configurable even if this is a fucking trait!
  private val expireMinutes: Int = 10
  private val maximumSizePerTable: Int = 1000

  private val cache = new HierarchicalCache(model, expireMinutes * 60 * 1000, maximumSizePerTable)

  override def createStorageTransaction(context: ExecutionContext) = new MysqlTransaction(this, Some(context)) with CachedMysqlTransaction {
    val transactionCache = cache.createTransactionCache
  }

  override def createStorageTransaction = new MysqlTransaction(this, None) with CachedMysqlTransaction {
    val transactionCache = cache.createTransactionCache
  }
}
