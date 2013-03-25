package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals
import com.wajam.mry.execution.Operation._

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

  def printTree(name: String): String = {
    val builder = new StringBuilder()

    builder append "--%s--\n".format(name)

    builder append "Id: %s, Addr: %s \n".format(this.id, System.identityHashCode(this))

    builder append "Variables: " + "\n"

    for (v <- variables)
      builder append "\tId: %s, Block: %s, Value: %s\n".format(v.id, System.identityHashCode(v.block), v.value)

    builder append "Operations: " + "\n"

    for (o <- operations)  {
      builder append "\tSource: %s, Type: %s\n".format(System.identityHashCode(o.source), o.getClass)

      o match {
        case _: Return =>

          val op = o.asInstanceOf[WithFrom]

          builder append "\tFrom: \n"

          op.from.foreach { (v) =>
            builder append "\t\tId: %s, Block: %s, Value: %s\n".format(v.id, System.identityHashCode(v.block), v.value)
          }

        case _: From  |
             _: Get |
             _: Projection |
             _: Limit =>

          val op = o.asInstanceOf[WithIntoAndKeys]
          val v = op.into

          builder append "\tInto: "
          builder append "Id: %s, Block: %s, Value: %s\n".format(v.id, System.identityHashCode(v.block), v.value)

          builder append "\tKeys: \n"

          op.keys.foreach { (v) => builder append "\t\t %s\n".format(v) }

        case _: Set |
             _: Delete =>

          val op = o.asInstanceOf[WithIntoAndData]
          val v = op.into

          builder append "\tInto: "
          builder append "Id: %s, Block: %s, Value: %s\n".format(v.id, System.identityHashCode(v.block), v.value)

          builder append "\tData: \n"

          op.data.foreach { (v) => builder append "\t\t%s\n".format(v) }
      }
    }

    builder.toString()
  }
}
