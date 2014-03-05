package com.wajam.mry.storage.mysql.cache

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers._
import scala.util.Random
import org.scalatest.FlatSpec

@RunWith(classOf[JUnitRunner])
class TestAccessPathOrdering extends FlatSpec {
  "AccessPathOrdering" should "compare" in new CacheSetup {
    AccessPathOrdering.compare(a_a.accessPath, aa_a.accessPath) should be(-1)
    AccessPathOrdering.compare(aa_a.accessPath, a_a.accessPath) should be(1)
    AccessPathOrdering.compare(a.accessPath, path("a")) should be(0)
  }

  it should "sort" in new CacheSetup {
    val sortedPaths = Random.shuffle(all.map(_.accessPath)).sorted(AccessPathOrdering)
    val expectedPaths = all.map(_.accessPath)
    sortedPaths should be(expectedPaths)
  }

  it should "verify ancestor" in new CacheSetup {
    AccessPathOrdering.isAncestor(a.accessPath, a.accessPath) === false
    AccessPathOrdering.isAncestor(a.accessPath, a_a.accessPath) === true
    AccessPathOrdering.isAncestor(a.accessPath, b.accessPath) === false
    AccessPathOrdering.isAncestor(b.accessPath, bb.accessPath) === false
    AccessPathOrdering.isAncestor(b_a.accessPath, b.accessPath) === false
    AccessPathOrdering.isAncestor(b_a.accessPath, b_a_a.accessPath) === false
    AccessPathOrdering.isAncestor(b.accessPath, b_a_a.accessPath) === true
  }
}
