package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange

/**
 * Feeder trait which iterate over table records sequentially per range of tokens. When started it resume from the last
 * acknowledged record saved in the task context. When it finish, it loops over and starts again with the first
 * range of data.
 */
trait TableContinuousFeeder extends TableFeederByToken {

  private[mysql] lazy val completedCount = metrics.counter("completed-count", name)

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord]) = {
    val loadRange = lastRecord match {
      case Some(record) => tokenRanges.find(token(record))
      case None => {
        currentRange match {
          case Some(range) => tokenRanges.next(range)
          case None => None
        }
      }
    }

    loadRange match {
      case Some(range) => (range, lastRecord)
      case None => {
        completedCount += 1
        (tokenRanges.head, None)
      }
    }
  }
}
