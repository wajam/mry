package com.appaquet.mry.execution


/**
 * Represents an operation executed within a transaction
 */
abstract class Operation(var source: ExecutionSource) extends Executable {
}

object Operation {
  def getTable(source: ExecutionSource, name: String, into: Variable): Operation = new GetTable(source, name, into)

  class GetTable(source: ExecutionSource, name: String, into: Variable) extends Operation(source) {
    def execute(context: ExecutionContext) {
      source.execGetTable(name, into)
    }

    def reset() {}
  }

  def get(source: ExecutionSource, key: Object, into: Variable) = new Get(source, key, into)

  class Get(source: ExecutionSource, key: Object, into: Variable) extends Operation(source) {
    def execute(context: ExecutionContext) {
      source.execGet(key, into)
    }

    def reset() {}
  }
}
