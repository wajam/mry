package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._

/**
 * MRY value representing a mysql table
 */
class TableValue(storage: MysqlStorage, table: Table, accessPrefix: AccessPath = new AccessPath()) extends Value {
  override def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
    if (keys.length > 0) {
      // get a specific row

      val key = param[StringValue](keys, 0).strValue
      val accessPath = new AccessPath(accessPrefix.parts ++ Seq(new AccessKey(key)))

      val token = context.getToken(accessPath(0).key)
      context.useToken(token)

      into.value = new RecordValue(storage, context, table, token, accessPath)

    } else {
      // get multiple rows

      if (accessPrefix.length == 0)
        throw new InvalidParameter("Can't get multiple rows (no key) on first level tables")

      // get token from first parameter
      val keysSeq = accessPrefix
      val token = context.getToken(keysSeq(0).key)
      into.value = new MultipleRecordValue(storage, context, table, token, accessPrefix)
    }

    if (!context.dryMode) {
      val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]
      transaction.markAsLazyRead(into.value)
    }
  }

  override def execGetKeys(context: ExecutionContext, into: Variable) {
    if (accessPrefix.length == 0)
      throw new InvalidParameter("Can't get multiple keys on first level tables")

    val keysSeq = accessPrefix
    val token = context.getToken(keysSeq(0).key)
    into.value = new MultipleKeyValue(storage, context, table, token, accessPrefix)
  }

  override def execSet(context: ExecutionContext, into: Variable, data: Object*) {
    val key = param[StringValue](data, 0).strValue
    val mapVal = param[MapValue](data, 1)
    val accessPath = new AccessPath(accessPrefix.parts ++ Seq(new AccessKey(key)))

    val token = context.getToken(accessPath(0).key)
    context.useToken(token)
    context.isMutation = true

    if (!context.dryMode) {
      val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]
      transaction.loadLazyValues()

      val record = new Record
      record.value = mapVal
      transaction.set(table, token, context.timestamp.get, accessPath, Some(record))
    }
  }

  override def execDelete(context: ExecutionContext, into: Variable, data: Object*) {
    val key = param[StringValue](data, 0).strValue
    val accessPath = new AccessPath(accessPrefix.parts ++ Seq(new AccessKey(key)))

    val token = context.getToken(accessPath(0).key)
    context.useToken(token)
    context.isMutation = true

    if (!context.dryMode) {
      val transaction = context.getStorageTransaction(storage).asInstanceOf[MysqlTransaction]
      transaction.loadLazyValues()

      transaction.set(table, token, context.timestamp.get, accessPath, None)
    }
  }

}


