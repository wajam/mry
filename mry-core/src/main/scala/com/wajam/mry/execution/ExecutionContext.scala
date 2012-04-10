package com.wajam.mry.execution

import com.wajam.mry.storage.{Storage, StorageTransaction => StorageTransaction}
import com.wajam.nrv.service.Resolver

/**
 * Execution context, used to store different information when a transaction
 * is executed.
 */
class ExecutionContext(var storages: Map[String, Storage]) {
  var dryMode:Boolean = false
  var isMutation:Boolean = false
  var timestamp = Timestamp.now
  var tokens: List[Long] = List()

  var storageTransactions = Map[Storage, StorageTransaction]()
  var returnValues: Seq[Value] = Seq()

  def getToken(data:String) = Resolver.hashData(data)

  def hasToken(data:String) = this.tokens.contains(this.getToken(data))

  def hasToken(token:Long) = this.tokens.contains(token)

  def useToken(data:String) { this.useToken(this.getToken(data)) }

  def useToken(token: Long) {
    if (!this.hasToken(token))
      this.tokens :+= token
  }

  def getStorage(name: String) = this.storages.getOrElse(name, throw new ExecutionException("No such storage %s".format(name)))

  def getStorageTransaction(storage: Storage): StorageTransaction = {
    if (!this.dryMode) {
      this.storageTransactions.getOrElse(storage, {
        val transaction = storage.getStorageTransaction(this)
        storageTransactions += (storage -> transaction)
        transaction
      })
    } else {
      throw new ExecutionException("A storage transaction has been requested for a dry execution")
    }
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
