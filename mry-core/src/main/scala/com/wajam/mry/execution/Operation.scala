package com.wajam.mry.execution

/**
 * Represents an operation executed within a transaction
 */
abstract class Operation(var source: OperationSource) extends Executable with Serializable {
}

object Operation {

  // TODO: To be replaced by proper hierachy when Java serialization in no longer used.
  type WithIntoAndData = { def into: Variable; def data: Seq[Object]}
  type WithIntoAndKeys = { def into: Variable; def keys: Seq[Object]}
  type WithFrom = { def from: Seq[Variable]}

  class Return(source: OperationSource, val from: Seq[Variable]) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execReturn(context, from)
    }

    def reset() {}
  }

  class From(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {

    def execute(context: ExecutionContext) {
      source.execFrom(context, into, keys: _*)
    }

    def reset() {}
  }

  class Get(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execGet(context, into, keys: _*)
    }

    def reset() {}
  }

  class Set(source: OperationSource, val into: Variable, val data: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execSet(context, into, data: _*)
    }

    def reset() {}
  }

  class Delete(source: OperationSource, val into: Variable, val data: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execDelete(context, into, data: _*)
    }

    def reset() {}
  }

  class Limit(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execLimit(context, into, keys: _*)
    }

    def reset() {}
  }

  class Projection(source: OperationSource, val into: Variable, val keys: Object*) extends Operation(source) with Serializable {
    def execute(context: ExecutionContext) {
      source.execProjection(context, into, keys: _*)
    }

    def reset() {}
  }
}

