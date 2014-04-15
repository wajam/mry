package com.wajam.mry.storage.mysql

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable
import com.wajam.spnl.feeder.Feeder.FeederData
import scala.language.implicitConversions

object FeederTestHelper {
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[FeederData]] with Closable = {

    new Iterator[Option[FeederData]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }

  implicit def onceFeederToIterator(feeder: Feeder with TableOnceFeeder): Iterator[Option[FeederData]] with Closable = {

    new Iterator[Option[FeederData]] with Closable {
      def hasNext = feeder.hasNext

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }
}