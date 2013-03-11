package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals

/**
 * MysqlTransaction (block of operations) executed on storage
 */
@SerialVersionUID(3228033012927254856L)
class Transaction(blockCreator: (Block with OperationApi) => Unit = null) extends Block with OperationApi with OperationSource with ContentEquals with Serializable {
  var id: Int = 0

  def sourceBlock = this

  if (blockCreator != null) {
    blockCreator(this)
  }

  override def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
    val storageName = param[StringValue](keys, 0).strValue


    into.value = storageName match {
      case "context" =>
        new context.ContextValue

      case _ =>
        val storage = context.getStorage(storageName)

        // get a transaction (not used here, but we need to get one to commit it at the end)
        if (!context.dryMode)
          context.getStorageTransaction(storage)

        // get storage value
        storage.getStorageValue(context)
    }
  }

  override def execReturn(context: ExecutionContext, from: Seq[Variable]) {
    context.returnValues = for (variable <- from) yield variable.value.serializableValue
  }

  override def equalsContent(obj: Any): Boolean = {
    obj match {
      case t: Transaction =>
        super.equalsContent(t) &&
        t.id == id
      case None => false
    }
  }

}
