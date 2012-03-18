package com.appaquet.mry.execution

/**
 * Variable that can points to nothing or to a value
 */
class Variable(var block:Block, var id:Int, var value:Option[Value] = None) extends Object with ExecutionSource {
  def sourceBlock = block

  def reset() {
    this.value = None
  }
}
