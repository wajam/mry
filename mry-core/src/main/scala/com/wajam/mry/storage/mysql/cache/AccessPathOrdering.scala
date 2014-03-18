package com.wajam.mry.storage.mysql.cache

import com.wajam.mry.storage.mysql.{AccessKey, AccessPath}

object AccessPathOrdering extends Ordering[AccessPath] {
  implicit private val AccessKeyOrdering = new Ordering[AccessKey] {
    def compare(x: AccessKey, y: AccessKey) = x.key.compareTo(y.key)
  }

  def compare(path1: AccessPath, path2: AccessPath) = {
    math.Ordering.Iterable[AccessKey].compare(path1.parts, path2.parts)
  }

  def isAncestor(ancestor: AccessPath, child: AccessPath): Boolean = {
    ancestor.length < child.length && ancestor.parts == child.parts.take(ancestor.length)
  }
}