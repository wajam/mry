package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.mry.storage.StorageException


/**
 * MRY value representing a mysql record
 */
abstract class BaseRecordValue[RecordType <: WithValue](storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                  accessPath: AccessPath, var optTransaction: Option[MysqlTransaction] = None,
                  var optRecord: Option[RecordType] = None) extends Value {

  if (optTransaction.isEmpty) {
    if (!context.dryMode) {
      optTransaction = Some(context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction])
    }
  }

  lazy val innerValue = {
    if (optRecord.isEmpty && !context.dryMode) {
      optRecord = executeTransaction()
    }

    this.optRecord match {
      case Some(r) =>
        r.value
      case None =>
        new NullValue
    }
  }

  // Serialized version of this record is the inner map or null
  override def serializableValue = this.innerValue

  // Operations are executed on record data (map or null)
  override def proxiedSource: Option[OperationSource] = Some(this.innerValue)

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
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

  def executeTransaction(): Option[RecordType]
}

class RecordValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                   accessPath: AccessPath, var transaction: Option[MysqlTransaction] = None,
                   var record: Option[Record] = None)
  extends BaseRecordValue[Record](storage, context, table, token, accessPath, transaction, record) {

  def executeTransaction() = optTransaction.get.get(table, token, context.timestamp.get, accessPath)
}

class KeyValue(storage: MysqlStorage, context: ExecutionContext, table: Table, token: Long,
                  accessPath: AccessPath, var transaction: Option[MysqlTransaction] = None,
                  var record: Option[Key] = None)
  extends BaseRecordValue[Key](storage, context, table, token, accessPath, transaction, record) {

  def executeTransaction() = optTransaction.get.getKey(table, token, context.timestamp.get, accessPath)
}


