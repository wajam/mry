package com.wajam.mry.execution

/**
 * Trait that makes an object source of the execution of an operation. All operations
 * are executed against something, this something must implements this trait.
 *
 * Ex: record.get("key"), record must be an execution source
 */
trait OperationSource {
  def proxiedSource: Option[OperationSource] = None

  private def getProxiedSource: OperationSource = {
    proxiedSource match {
      case None => throw new UnsupportedExecutionSource
      case Some(o) => o
    }
  }

  def execReturn(context: ExecutionContext, from: Seq[Variable]) {
    getProxiedSource.execReturn(context, from)
  }

  def execFromTable(context: ExecutionContext, name: String, into: Variable) {
    getProxiedSource.execFromTable(context, name, into)
  }

  def execGet(context: ExecutionContext, key: Object, into: Variable) {
    getProxiedSource.execGet(context, key, into)
  }

  def execSet(context: ExecutionContext, key: Object, value: Object, into: Variable) {
    getProxiedSource.execSet(context, key, value, into)
  }

  class UnsupportedExecutionSource extends Exception

}
