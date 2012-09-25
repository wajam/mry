package com.wajam.mry.storage

import com.wajam.mry.execution._

/**
 * In memory storage engine
 */
class MemoryStorage(name: String) extends Storage(name) {
  var globData = Map[String, Value]()

  def nuke() {}

  def start() {}

  def stop() {}

  def createStorageTransaction(context: ExecutionContext) = new MemoryTransaction(context)

  def getStorageValue(context: ExecutionContext): Value = new StorageValue

  class MemoryTransaction(context: ExecutionContext) extends StorageTransaction {
    var data = Map[String, Option[Value]]()

    def rollback() {}

    def commit() {
      for ((k, v) <- this.data) {
        v match {
          case Some(x) =>
            globData += (k -> x)
          case None =>
            globData -= (k)
        }
      }
    }
  }

  class StorageValue extends Value {
    override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
      val key = param[StringValue](keys, 0).strValue
      context.useToken(key)

      if (!context.dryMode) {
        val transaction = context.getStorageTransaction(MemoryStorage.this).asInstanceOf[MemoryTransaction]
        val value = transaction.data.getOrElse(key, globData.get(key))
        value match {
          case Some(v) => into.value = v
          case None => into.value = new NullValue
        }
      }
    }

    override def execSet(context: ExecutionContext, into: Variable, data: Object*) {
      val key = param[StringValue](data, 0).strValue
      val value = param[Value](data, 1)

      context.useToken(key)
      context.isMutation = true

      if (!context.dryMode) {
        val transaction = context.getStorageTransaction(MemoryStorage.this).asInstanceOf[MemoryTransaction]
        transaction.data += (key -> Some(value.value))
      }
    }

    override def execDelete(context: ExecutionContext, into: Variable, data: Object*) {
      val key = param[StringValue](data, 0).strValue

      context.useToken(key)
      context.isMutation = true

      if (!context.dryMode) {
        val transaction = context.getStorageTransaction(MemoryStorage.this).asInstanceOf[MemoryTransaction]
        transaction.data += (key -> None)
      }
    }
  }

}
