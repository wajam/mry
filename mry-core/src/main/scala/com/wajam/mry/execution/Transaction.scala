package com.wajam.mry.execution

/**
 * MysqlTransaction (block of operations) executed on storage
 */
class Transaction(blockCreator:(Block with OperationApi)=>Unit = null) extends Block with OperationApi with OperationSource with Serializable {
  var id: Int = 0

  def sourceBlock = this

  if (blockCreator != null) {
    blockCreator(this)
  }

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
    val storageName = param[StringValue](keys, 0)
    val storage = context.getStorage(storageName.strValue)

    // get a transaction (not used here, but we need to get one to commit it at the end)
    if (!context.dryMode)
      context.getStorageTransaction(storage)

    // get storage value
    into.value = storage.getStorageValue(context)
  }

  override def execReturn(context: ExecutionContext, from: Seq[Variable]) {
    context.returnValues = for (variable <- from) yield variable.value.serializableValue
  }
}
