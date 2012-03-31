package com.wajam.mry.storage

import com.wajam.mry.execution._

/**
 * In memory storage engine
 */
class MemoryStorage(name: String) extends Storage(name) {
  var globData = Map[String, Value]()

  def getStorageTransaction(time: Timestamp) = new MemoryTransaction

  def getStorageValue(transaction: StorageTransaction, context: ExecutionContext): Value = transaction.asInstanceOf[MemoryTransaction]

  def nuke() {}

  def close() {}

  class MemoryTransaction extends StorageTransaction with Value {
    var trxData = Map[String, Option[Value]]()

    override def execSet(context: ExecutionContext, into: Variable, value: Object, keys: Object*) {
      val key = param[StringValue](keys, 0).strValue
      this.trxData += (key -> Some(value.value))
    }

    override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
      val key = param[StringValue](keys, 0).strValue

      val value = this.trxData.getOrElse(key, globData.get(key))
      value match {
        case Some(v) => into.value = v
        case None => into.value = new NullValue
      }
    }

    def rollback() {}

    def commit() {
      for ((k, v) <- this.trxData) {
        v match {
          case Some(x) =>
            globData += (k -> x)
          case None =>
            globData -= (k)
        }
      }
    }
  }

}
