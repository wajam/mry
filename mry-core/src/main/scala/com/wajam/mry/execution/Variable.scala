package com.wajam.mry.execution

/**
 * Variable that can points to nothing or to a value
 */
@SerialVersionUID(-2511774283683367275L)
class Variable(block: Block, id: Int, var value: Value = NullValue) extends Object with OperationSource with OperationApi with Serializable {
  def sourceBlock = block

  override def proxiedSource = Some(value)

  def reset() {
    this.value = NullValue
  }
}
