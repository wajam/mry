package com.wajam.mry.api

import com.wajam.mry.execution.Operation._
import com.wajam.mry.execution.{Variable, Transaction}

/**
  * Generate a very detailed pretty print of a Transaction tree.
  * Used for debugging.
  *
  */
object TransactionPrinter {

  def printTree(transaction: Transaction, contextName: String): String = {
    val builder = new StringBuilder()

    builder append "--%s--\n".format(contextName)

    builder append "Id: %s, Addr: %s \n".format(transaction.id, System.identityHashCode(transaction))

    builder append "VarSeq: %d\n".format(transaction.varSeq)
    builder append "Parent: %s\n".format(System.identityHashCode(transaction.parent.getOrElse(null)))
    builder append "Variables: " + "\n"

    for (v <- transaction.variables)
      builder append "\tId: %s, Block: %s, Value: %s, Addr: %s\n".format(v.id, System.identityHashCode(v.block), v.value, System.identityHashCode(v))

    builder append "Operations: " + "\n"

    for (o <- transaction.operations)  {
      builder append "\tSource: %s, Type: %s, Addr: %s\n".format(System.identityHashCode(o.source), o.getClass, System.identityHashCode(o))

      val printWithFrom = (from: Seq[Variable]) => {
        builder append "\tFrom: \n"

        from.foreach { (v) =>
          builder append "\t\tId: %s, Block: %s, Value: %s, Addr: %s\n".format(v.id, System.identityHashCode(v.block), v.value, System.identityHashCode(v))
        }
      }

      val printWithIntoAndSeqObject = (into: Variable, objects: Seq[Object]) => {
        val v = into

        builder append "\tInto: "
        builder append "Id: %s, Block: %s, Value: %s, Addr: %s\n".format(v.id, System.identityHashCode(v.block), v.value, System.identityHashCode(v))

        builder append "\tKeys\\Data: \n"

        objects.foreach { (v) => builder append "\t\t%s, Addr: %s\n".format(v, System.identityHashCode(v)) }
      }

      o match {
        case op: Return => printWithFrom(op.from)
        case op: From => printWithIntoAndSeqObject(op.into, op.keys)
        case op: Get => printWithIntoAndSeqObject(op.into, op.keys)
        case op: Set => printWithIntoAndSeqObject(op.into, op.data)
        case op: Delete => printWithIntoAndSeqObject(op.into, op.data)
        case op: Limit => printWithIntoAndSeqObject(op.into, op.keys)
        case op: Projection => printWithIntoAndSeqObject(op.into, op.keys)
      }
    }

    builder.toString()
  }

}
