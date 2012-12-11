package com.wajam.mry.execution

/**
 * Represents an operation executed within a transaction
 */
abstract class Operation(var source: OperationSource) extends Executable with Serializable {
}

object Operation {

  class Return(source: OperationSource, from: Seq[Variable]) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execReturn(context, from)
    }

    def reset() {}
  }

  class From(source: OperationSource, into: Variable, keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execFrom(context, into, keys: _*)
    }

    def reset() {}
  }

  class Get(source: OperationSource, into: Variable, keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execGet(context, into, keys: _*)
    }

    def reset() {}
  }

  class Set(source: OperationSource, into: Variable, data: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execSet(context, into, data: _*)
    }

    def reset() {}
  }

  class Delete(source: OperationSource, into: Variable, data: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execDelete(context, into, data: _*)
    }

    def reset() {}
  }

  class Limit(source: OperationSource, into: Variable, keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execLimit(context, into, keys: _*)
    }

    def reset() {}
  }

}

