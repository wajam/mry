package com.wajam.mry.execution

import com.wajam.mry.execution.Operation.{Return, Set, Get, From}


/**
 * Description
 */

trait OperationApi extends OperationSource {
  def sourceBlock: Block

  def ret(from: Variable*) {
    sourceBlock.addOperation(new Return(this, from))
  }

  def returns(from: Variable*) {
    sourceBlock.addOperation(new Return(this, from))
  }

  def from(keys: Object*): Variable = {
    this.fromInto(this.sourceBlock.defineVariable(), keys: _*)
  }

  def fromInto(into: Variable, keys: Object*): Variable = {
    sourceBlock.addOperation(new From(this, into, keys: _*))
    into
  }

  def get(keys: Object*): Variable = {
    this.getInto(this.sourceBlock.defineVariable(), keys: _*)
  }

  def getInto(into: Variable, keys: Object*): Variable = {
    sourceBlock.addOperation(new Get(this, into, keys: _*))
    into
  }

  def set(value: Object, keys: Object*): Variable = {
    this.setInto(this.sourceBlock.defineVariable(), value, keys: _*)
  }

  def setInto(into: Variable, value: Object, keys: Object*): Variable = {
    sourceBlock.addOperation(new Set(this, into, value, keys: _*))
    into
  }
}
