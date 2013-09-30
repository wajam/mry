package com.wajam.mry.storage.mysql

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable

object FeederTestHelper {
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[Map[String, Any]]] with Closable = {

    new Iterator[Option[Map[String, Any]]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }
}