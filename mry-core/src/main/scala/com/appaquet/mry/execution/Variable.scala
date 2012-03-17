package com.appaquet.mry.execution

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

class Variable(var block:Block, var id:Int, var value:Option[Value] = None) extends Object with ExecutionSource {
  def sourceBlock = block

  def reset() {
    this.value = None
  }
}
