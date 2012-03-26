package com.appaquet.mry.execution

/**
 * Variable that can points to nothing or to a value
 */
class Variable(var block:Block, var id:Int, var value:Value = new NullValue) extends Object with OperationSource with OperationApi {
  def sourceBlock = block
  override def proxiedSource = Some(value)

  def reset() {
    this.value = new NullValue
  }
}
