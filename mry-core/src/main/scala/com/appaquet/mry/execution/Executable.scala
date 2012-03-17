package com.appaquet.mry.execution

/**
 * DESCRIPTION HERE
 *
 * Author: Andre-Philippe Paquet < app@quet.ca >
 */

trait Executable {
  def execute(context:Context)

  def reset()
}
