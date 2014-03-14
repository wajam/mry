package com.wajam.mry.execution

import com.wajam.mry.storage.{Storage, StorageTransaction => StorageTransaction}
import com.wajam.nrv.service.Resolver
import com.wajam.commons.Logging
import com.wajam.nrv.cluster.Cluster
import com.wajam.nrv.utils.timestamp.Timestamp

/**
 * Execution context, used to store different information when a transaction
 * is executed.
 */
class ExecutionContext(var storages: Map[String, Storage], val timestamp: Option[Timestamp] = None) extends Logging {
  var dryMode: Boolean = false
  var isMutation: Boolean = false
  var tokens: List[Long] = List()
  var cluster: Cluster = null
  var cacheAllowed: Boolean = true

  var storageTransactions = Map[Storage, StorageTransaction]()
  var returnValues: Seq[Value] = Seq()

  def getToken(data: String) = Resolver.hashData(data)

  def hasToken(data: String) = this.tokens.contains(this.getToken(data))

  def hasToken(token: Long) = this.tokens.contains(token)

  def useToken(data: String) {
    this.useToken(this.getToken(data))
  }

  def useToken(token: Long) {
    if (!this.hasToken(token))
      this.tokens :+= token
  }

  def getStorage(name: String) = this.storages.getOrElse(name, throw new ExecutionException("No such storage %s".format(name)))

  def getStorageTransaction(storage: Storage): StorageTransaction = {
    if (!this.dryMode) {
      this.storageTransactions.getOrElse(storage, {
        val transaction = storage.createStorageTransaction(this)
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

  class ContextValue extends NullValue {
    override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
      val key = param[StringValue](keys, 0)

      key.strValue match {
        case "tokens" =>
          into.value = ListValue(for (token <- context.tokens) yield IntValue(token))

        case "local_node" =>
          if (context.cluster != null)
            into.value = StringValue(context.cluster.localNode.uniqueKey)

        case _ =>
          throw new ExecutionException("Only 'tokens' can be get from transaction block")
      }
    }
  }

}
