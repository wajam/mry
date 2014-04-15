package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange
import com.github.nscala_time.time.Imports._

/**
 * Feeder trait which iterate over table records sequentially per range of tokens. When started it resume from the last
 * acknowledged record saved in the task context. When it finish, it stops.
 */
trait TableOnceFeeder extends TableFeederByToken {

  private def startDate = context.data.get("start-date").map(_.asInstanceOf[DateTime])
  private def finishedDate = context.data.get("finished-date").map(_.asInstanceOf[DateTime])

  private[mysql] def hasNext: Boolean = {
    startDate match {
      case Some(date) => finishedDate.forall(_ < date) // Finished date, if defined, is older than start date
      case None => false
    }
  }

  private def setDone(): Unit = {
    context.data = context.data.updated("finished-date", DateTime.now)
  }

  private def emptyResult =
    (TokenRange(1, 0), lastRecord) // start is after end

  def start(): Boolean = {
    if (hasNext) false else {
      context.data = context.data.updated("start-date", DateTime.now)
      true
    }
  }

  def reset(): Boolean = {
    if (hasNext) false else {
      lastRecord = None
      currentRange = None
      true
    }
  }

  def restart(): Boolean = reset() && start()

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord]) = {
    if (hasNext) {
      val loadRange = lastRecord match {
        case Some(record) => tokenRanges.find(token(record))
        case None => currentRange match {
          case Some(range) => tokenRanges.next(range) orElse {
            setDone()
            None
          }
          case None => None
        }
      }

      loadRange match {
        case Some(range) => (range, lastRecord)
        case None if hasNext => (tokenRanges.head, None)
        case None => emptyResult
      }
    } else emptyResult
  }
}
