package com.wajam.mry.execution

import com.wajam.mry.execution.Operation.Return
import collection.mutable.ArrayBuffer

/**
 * Programmatic block of operations and variables that is executed against storage
 */
trait Block {
  //TODO should not expose these publicly, wait til Protobuf MRY codec is done
  var variables = ArrayBuffer[Variable]()
  var operations = ArrayBuffer[Operation]()
  var varSeq = 0
  var parent: Option[Block] = None

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
}
