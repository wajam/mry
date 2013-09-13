package com.wajam.mry.storage.mysql

import com.wajam.nrv.utils.timestamp.Timestamp

trait CachedMysqlTransaction
  extends MysqlTransaction {

  val cacheTransaction: HierarchicalCache#CacheTransaction

  override def get(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath, includeDeleted: Boolean): Option[Record] =
    cacheTransaction.getOrSet(table, accessPath, {
      super.get(table, token, timestamp, accessPath, includeDeleted)
    })

  override def set(table: Table, token: Long, timestamp: Timestamp, accessPath: AccessPath, optRecord: Option[Record]) {
    cacheTransaction.put(table, accessPath, {
      super.set(table, token, timestamp, accessPath, optRecord)
      optRecord
    })
  }

  override def commit() {
    super.commit()
    cacheTransaction.commit()
  }
}
