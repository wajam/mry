package com.wajam.mry.execution

import com.wajam.mry.execution.Operation.Return
import collection.mutable.ArrayBuffer
import com.wajam.nrv.utils.ContentEquals

/**
 * Programmatic block of operations and variables that is executed against storage
 */
trait Block {
trait Block extends ContentEquals  {

  private[mry] val variables = ArrayBuffer[Variable]()
  private[mry] val operations = ArrayBuffer[Operation]()
  private[mry] var varSeq = 0
  private[mry] var parent: Option[Block] = None

  def defineVariable(count: Int): Seq[Variable] = {
    val newVars = for (i <- 0 until count) yield new Variable(this, this.varSeq + i)
    this.varSeq += count
    this.variables ++= newVars

    newVars
  }

  def defineVariable(): Variable = {
    val variable = new Variable(this, this.varSeq)
    this.variables += variable
    this.varSeq += 1
    variable
  }

  def addOperation(operation: Operation) {
    this.operations += operation
  }

  def reset() {
    for (variable <- this.variables) variable.reset()
    for (operation <- this.operations) operation.reset()
  }

  def execute(context: ExecutionContext, commit:Boolean = false) {
    for (operation <- this.operations) {
      operation.execute(context)

      // break execution if it's return
      if (operation.isInstanceOf[Return]) {
        return
      }
    }

    if (commit) context.commit()
  }

  override def equalsContent(obj: Any): Boolean = {
    obj match {
      case b: Block =>
        variables.zip(b.variables).forall((zip) => zip._1 equalsContent(zip._2)) &&
        operations.zip(b.operations).forall((zip) => zip._1 equalsContent(zip._2)) &&
        varSeq == b.varSeq
      case None => false
    }
  }
}
