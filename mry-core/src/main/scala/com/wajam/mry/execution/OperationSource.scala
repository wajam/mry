package com.wajam.mry.execution

import com.wajam.nrv.utils.ContentEquals

/**
 * Trait that makes an object source of the execution of an operation. All operations
 * are executed against something, this something must implements this trait.
 *
 * Ex: record.get("key"), record must be an execution source
 */
trait OperationSource extends ContentEquals  {
  def proxiedSource: Option[OperationSource] = None

  private def getProxiedSource: OperationSource = {
    proxiedSource match {
      case None => throw new UnsupportedExecutionSource
      case Some(o) => o
    }
  }

  def param[T: ClassManifest](param: Object): T = {
    this.param[T](Seq(param), 0)
  }

  def param[T: ClassManifest](params: Seq[Object], position: Int): T = {
    if (params.size <= position)
      throw new InvalidParameter("Excepted parameter at position %d".format(position))

    val param = params(position).value
    if (!param.isInstanceOf[T])
      throw new InvalidParameter("Excepted parameter at position %d to be of instance %s".format(position, classManifest[T].erasure.getName))

    param.asInstanceOf[T]
  }

  def execReturn(context: ExecutionContext, from: Seq[Variable]) {
    getProxiedSource.execReturn(context, from)
  }

  def execFrom(context: ExecutionContext, into: Variable, keys: Object*) {
    getProxiedSource.execFrom(context, into, keys: _*)
  }

  def execGet(context: ExecutionContext, into: Variable, keys: Object*) {
    getProxiedSource.execGet(context, into, keys: _*)
  }

  def execSet(context: ExecutionContext, into: Variable, data: Object*) {
    getProxiedSource.execSet(context, into, data: _*)
  }

  def execDelete(context: ExecutionContext, into: Variable, keys: Object*) {
    getProxiedSource.execDelete(context, into, keys: _*)
  }

  def execLimit(context: ExecutionContext, into: Variable, keys: Object*) {
    getProxiedSource.execLimit(context, into, keys: _*)
  }

  def execProjection(context: ExecutionContext, into: Variable, keys: Object*) {
    getProxiedSource.execProjection(context, into, keys: _*)
  }

  def execFiltering(context: ExecutionContext, into: Variable, key: Object, filter: MryFilters.MryFilter, value: Object) {
    getProxiedSource.execFiltering(context, into, key, filter, value)
  }

  def execPredicate(context: ExecutionContext, into: Variable, key: Object, filter: MryFilters.MryFilter, value: Object) {
    getProxiedSource.execPredicate(context, into, key, filter, value)
  }
}

class InvalidParameter(reason: String) extends Exception("%s: %s".format(getClass.toString, reason))

class UnsupportedExecutionSource extends Exception(getClass.toString)

