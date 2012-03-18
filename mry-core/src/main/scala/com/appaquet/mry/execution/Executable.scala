package com.appaquet.mry.execution

/**
 * Marks an object has executable
 */
trait Executable {
  def execute(context:ExecutionContext)

  def reset()
}
