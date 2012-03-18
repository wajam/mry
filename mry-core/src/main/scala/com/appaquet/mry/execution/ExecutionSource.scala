package com.appaquet.mry.execution

/**
 * Trait that makes an object source of the execution of an operation. All operations
 * are executed against something, this something must implements this trait.
 *
 * Ex: record.get("key"), record must be an execution source
 */
trait ExecutionSource {
  def sourceBlock: Block

  def FromTable(name: String, into: Variable = this.sourceBlock.defineVariable()): Variable = {
    sourceBlock.addOperation(Operation.getTable(this, name, into))
    into
  }

  def execGetTable(name: String, into: Variable) {
    throw new UnsupportedExecutionSource
  }

  def Get(key: Object, into: Variable = this.sourceBlock.defineVariable()): Variable = {
    sourceBlock.addOperation(Operation.get(this, key, into))
    into
  }

  def execGet(key: Object, into: Variable) {
    throw new UnsupportedExecutionSource
  }

  class UnsupportedExecutionSource extends Exception

}
