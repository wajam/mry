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

      // get current value to get generation to use
      accessPath.last.generation = Some(this.getGeneration(transaction, token, context.timestamp, accessPath))

      val record = new Record
      record.value = mapVal
      transaction.set(table, token, context.timestamp, accessPath, Some(record))
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

      // get current value to get generation to use
      accessPath.last.generation = Some(this.getGeneration(transaction, token, context.timestamp, accessPath))

      transaction.set(table, token, context.timestamp, accessPath, None)
    }
  }

  private def getGeneration(transaction:MysqlTransaction, token:Long, timestamp:Timestamp, accessPath:AccessPath):Int = {
    val curRec = transaction.get(table, token, timestamp, accessPath, includeDeleted = true)
    if (curRec.isDefined) {
      if (!curRec.get.value.isNull)
        curRec.get.accessPath.last.generation.get
      else
        curRec.get.accessPath.last.generation.get + 1
    } else {
      1
    }
  }

}


