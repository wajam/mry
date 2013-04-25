package com.wajam.mry.execution

import com.wajam.mry.execution.Operation._


/**
 * Trait that makes it possible to execute operation on an object
 */

trait OperationApi extends OperationSource {
  def sourceBlock: Block

  def ret(from: Variable*) {
    sourceBlock.addOperation(new Return(this, from))
  }

  def returns(from: Variable*) {
    sourceBlock.addOperation(new Return(this, from))
  }

  def from(keys: Object*): Variable = {
    this.fromInto(this.sourceBlock.defineVariable(), keys: _*)
  }

  def fromInto(into: Variable, keys: Object*): Variable = {
    sourceBlock.addOperation(new From(this, into, keys: _*))
    into
  }

  def get(keys: Object*): Variable = {
    this.getInto(this.sourceBlock.defineVariable(), keys: _*)
  }

  def getInto(into: Variable, keys: Object*): Variable = {
    sourceBlock.addOperation(new Get(this, into, keys: _*))
    into
  }

  def set(data: Object*): Variable = {
    this.setInto(this.sourceBlock.defineVariable(), data: _*)
  }

  def setInto(into: Variable, data: Object*): Variable = {
    sourceBlock.addOperation(new Set(this, into, data: _*))
    into
  }

  def delete(data: Object*): Variable = {
    this.deleteInto(this.sourceBlock.defineVariable(), data: _*)
  }

  def deleteInto(into: Variable, data: Object*): Variable = {
    sourceBlock.addOperation(new Delete(this, into, data: _*))
    into
  }

  def limit(keys: Object*): Variable = {
    this.limitInto(this.sourceBlock.defineVariable(), keys: _*)
  }

  def limitInto(into: Variable, keys: Object*): Variable = {
    sourceBlock.addOperation(new Limit(this, into, keys: _*))
    into
  }

  def projection(keys: Object*): Variable = {
    this.projectionInto(this.sourceBlock.defineVariable(), keys: _*)
  }

  def filter(key: Object, filter: MryFilters.MryFilter, value: Value): Variable = {
    this.filterInto(this.sourceBlock.defineVariable(), key, filter, value)
  }

  def filterInto(into: Variable, key: Object, filter: MryFilters.MryFilter, value: Value): Variable = {
    sourceBlock.addOperation(new Filter(this, into, key, filter, value))
    into
  }

  def projectionInto(into: Variable, keys: Object*): Variable = {
    sourceBlock.addOperation(new Projection(this, into, keys: _*))
    into
  }
}
