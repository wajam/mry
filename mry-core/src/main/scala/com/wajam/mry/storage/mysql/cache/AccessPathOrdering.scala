package com.wajam.mry.storage.mysql.cache

import com.wajam.mry.storage.mysql.{AccessKey, AccessPath}
import scala.annotation.tailrec

object AccessPathOrdering extends Ordering[AccessPath] {
  def compare(path1: AccessPath, path2: AccessPath) = {
    compare(path1.parts, path2.parts)
  }

  def isAncestor(ancestor: AccessPath, child: AccessPath): Boolean = {
    ancestor.length < child.length && ancestor.parts == child.parts.take(ancestor.length)
  }

  @tailrec
  private def compare(keys1: Seq[AccessKey], keys2: Seq[AccessKey]): Int = {
    (keys1, keys2) match {
      case (Nil, Nil) => 0
      case (Nil, _) => -1
      case (_, Nil) => 1
      case (h1 :: t1, h2 :: t2) => {
        val result = h1.key.compareTo(h2.key)
        if (result == 0) compare(t1, t2) else result
      }
    }
  }
}