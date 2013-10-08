package com.wajam.mry.storage.mysql

import com.wajam.spnl.feeder.Feeder
import com.wajam.commons.Closable
import com.wajam.spnl.TaskData

object FeederTestHelper {
  implicit def feederToIterator(feeder: Feeder): Iterator[Option[TaskData]] with Closable = {

    new Iterator[Option[TaskData]] with Closable {
      def hasNext = true

      def next() = feeder.next()

      def close() = feeder.kill()
    }
  }
}