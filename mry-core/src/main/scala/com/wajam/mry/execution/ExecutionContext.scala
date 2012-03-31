package com.wajam.mry.execution

import com.wajam.mry.storage.{Storage, StorageTransaction => StorageTransaction}

/**
 * Execution context, used to store different information when a transaction
 * is executed.
 */
class ExecutionContext(var storages: Map[String, Storage]) {
  var timestamp = Timestamp.now
  var storageTransactions = Map[Storage, StorageTransaction]()
  var returnValues: Seq[Value] = Seq()

  def getStorage(name: String) = this.storages(name)

  def getStorageTransaction(storage: Storage): StorageTransaction = {
    this.storageTransactions.getOrElse(storage, {
      val transaction = storage.getStorageTransaction(timestamp)
      storageTransactions += (storage -> transaction)
      transaction
    })
  }

  def commit() {
    for ((s, t) <- storageTransactions) {
      t.commit()
    }
  }

  def rollback() {
    for ((s, t) <- storageTransactions) {
      t.rollback()
    }
  }
}
