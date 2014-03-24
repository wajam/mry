package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.mry.storage.StorageException


/**
 * MRY value representing a mysql record
 */
class RecordValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                  accessPath: AccessPath, var optTransaction: Option[MysqlTransaction] = None,
                  var optRecord: Option[Record] = None) extends Value {

  if (optTransaction.isEmpty) {
    if (!context.dryMode) {
      optTransaction = Some(context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction])
    }
  }

  lazy val innerValue = {
    if (optRecord.isEmpty && !context.dryMode) {
      optRecord = optTransaction.get.get(table, token, context.timestamp.get, accessPath)
    }

    this.optRecord match {
      case Some(r) =>
        r.value
      case None =>
        NullValue
    }
  }

  // Serialized version of this record is the inner map or null
  override def serializableValue = this.innerValue

  // Operations are executed on record data (map or null)
  override def proxiedSource: Option[OperationSource] = Some(this.innerValue)

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*): Unit = {
    val tableName = param[StringValue](keys, 0).strValue
    val optTable = table.getTable(tableName)

    if (!context.dryMode && innerValue.isNull)
      throw new StorageException("Cannot execute 'from' on an non existing record (table=%s, access_path=%s)".format(table.depthName("_"), accessPath))

    optTable match {
      case Some(t) =>
        into.value = new TableValue(storage, t, accessPath)

      case None =>
        throw new StorageException("Non existing table %s".format(tableName))

    }
  }

  override def execSet(context: ExecutionContext, into: Variable, data: Object*): Unit = {
    if (context.dryMode) {
      into.value = MapValue(Map())
    } else {
      innerValue.execSet(context, into, data: _*)
    }
  }
}


