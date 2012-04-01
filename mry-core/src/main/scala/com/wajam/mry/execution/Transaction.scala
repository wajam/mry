package com.wajam.mry.execution

/**
 * MysqlTransaction (block of operations) executed on storage
 */
class Transaction(var id: Int = 0) extends Block with OperationApi with OperationSource {
  def sourceBlock = this

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
    val storageName = param[StringValue](keys, 0)

    val storage = context.getStorage(storageName.strValue)
    val storageTransaction = context.getStorageTransaction(storage)
    into.value = storage.getStorageValue(storageTransaction, context)
  }

  override def execReturn(context: ExecutionContext, from: Seq[Variable]) {
    context.returnValues = for (variable <- from) yield variable.value.serializableValue
  }
}
