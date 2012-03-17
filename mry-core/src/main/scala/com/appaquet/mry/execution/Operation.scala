package com.appaquet.mry.execution


/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

abstract class Operation(var source: ExecutionSource) extends Executable {
}

object Operation {
  def getTable(source: ExecutionSource, name: String, into: Variable): Operation = new GetTable(source, name, into)

  class GetTable(source: ExecutionSource, name: String, into: Variable) extends Operation(source) {
    def execute(context: Context) {
      source.execGetTable(name, into)
    }

    def reset() {}
  }

  def get(source: ExecutionSource, key: Object, into: Variable) = new Get(source, key, into)

  class Get(source: ExecutionSource, key: Object, into: Variable) extends Operation(source) {
    def execute(context: Context) {
      source.execGet(key, into)
    }

    def reset() {}
  }
}
