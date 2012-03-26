package com.appaquet.mry.execution

import com.appaquet.mry.execution.Operation.{Return, Set, Get, GetTable}


/**
 * Description
 */

trait OperationApi extends OperationSource {
  def sourceBlock: Block

  def ret(from: Variable*) {
    sourceBlock.addOperation(new Return(this, from))
  }

  def from(name: String, into: Variable = this.sourceBlock.defineVariable()): Variable = {
    sourceBlock.addOperation(new GetTable(this, name, into))
    into
  }

  def get(key: Object, into: Variable = this.sourceBlock.defineVariable()): Variable = {
    sourceBlock.addOperation(new Get(this, key, into))
    into
  }

  def set(key: Object, value: Object, into: Variable = this.sourceBlock.defineVariable()): Variable = {
    sourceBlock.addOperation(new Set(this, key, value, into))
    into
  }
}
