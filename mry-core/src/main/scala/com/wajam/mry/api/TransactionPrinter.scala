package com.wajam.mry.api

import com.wajam.mry.execution.Operation._
import com.wajam.mry.execution.Transaction

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

      o match {
        case _: Return =>

          val op = o.asInstanceOf[WithFrom]

          builder append "\tFrom: \n"

          op.from.foreach { (v) =>
            builder append "\t\tId: %s, Block: %s, Value: %s, Addr: %s\n".format(v.id, System.identityHashCode(v.block), v.value, System.identityHashCode(v))
          }

        case _: From  |
             _: Get |
             _: Projection |
             _: Limit =>

          val op = o.asInstanceOf[WithIntoAndKeys]
          val v = op.into

          builder append "\tInto: "
          builder append "Id: %s, Block: %s, Value: %s, Addr: %s\n".format(v.id, System.identityHashCode(v.block), v.value, System.identityHashCode(v))

          builder append "\tKeys: \n"

          op.keys.foreach { (v) => builder append "\t\t%s, Addr: %s\n".format(v, System.identityHashCode(v)) }

        case _: Set |
             _: Delete =>

          val op = o.asInstanceOf[WithIntoAndData]
          val v = op.into

          builder append "\tInto: "
          builder append "Id: %s, Block: %s, Value: %s, Addr: %s\n".format(v.id, System.identityHashCode(v.block), v.value, System.identityHashCode(v))

          builder append "\tData: \n"

          op.data.foreach { (v) => builder append "\t\t%s, Addr: %s\n".format(v, System.identityHashCode(v)) }
      }
    }

    builder.toString()
  }

}
