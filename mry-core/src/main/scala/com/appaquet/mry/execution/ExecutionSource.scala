package com.appaquet.mry.execution


/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
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
