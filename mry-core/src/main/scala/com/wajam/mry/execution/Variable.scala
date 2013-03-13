package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals

/**
 * Variable that can points to nothing or to a value
 */
@SerialVersionUID(-2511774283683367275L)
class Variable (private[mry] val block: Block, private[mry] val id: Int, var value: Value = NullValue) extends Object with OperationSource with OperationApi with ContentEquals with Serializable {
  def sourceBlock = block

  override def proxiedSource = Some(value)

  def reset() {
    this.value = NullValue
  }

  override def equalsContent(obj: Any): Boolean = {
    obj match {
      case v: Variable =>
        value.equalsValue(v.value)

      case None => false
    }
  }
}
