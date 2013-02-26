package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import scala.Some

/**
 * 
 */
class KeyValue (storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                accessPath: AccessPath, var optTransaction: Option[MysqlTransaction] = None,
                var optKey: Option[Key] = None) extends Value {

  if (optTransaction.isEmpty) {
    if (!context.dryMode) {
      optTransaction = Some(context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction])
    }
  }

  lazy val innerValue = {
    if (optKey.isEmpty && !context.dryMode) {
      optKey = optTransaction.get.getKey(table, token, context.timestamp.get, accessPath)
    }

    this.optKey match {
      case Some(r) =>
        r.value
      case None =>
        new NullValue
    }
  }

  // Serialized version of this key is the inner string or null
  override def serializableValue = this.innerValue

  // Operations are executed on key data (string or null)
  override def proxiedSource: Option[OperationSource] = Some(this.innerValue)
}
