package com.wajam.mry.storage.mysql

import com.wajam.nrv.service.TokenRange
import com.github.nscala_time.time.Imports._
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext
import org.joda.time.format.ISODateTimeFormat

/**
 * Feeder trait which iterate over table records sequentially per range of tokens. When started it resume from the last
 * acknowledged record saved in the task context. When it finish, it stops.
 */
trait TableOnceFeeder extends TableFeederByToken with Job {
  import TableOnceFeeder._

  private def getStartDate = context.data.get(startDate).map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))
  private def getFinishedDate = context.data.get(finishedDate).map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))

  @volatile private[this] var currentId = 0
  @volatile private[this] var stoppedRequested: Boolean = false
  private[this] var contextDataToUpdate = Map.empty[String, String]

  def dateFormatter = ISODateTimeFormat.dateTime()

  private[mysql] def hasNext: Boolean = {
    getStartDate match {
      case Some(date) => getFinishedDate.forall(_ < date) // Finished date, if defined, is older than start date
      case None => false
    }
  }

  private def setDone(): Unit = {
    updateContext(Map(finishedDate -> dateFormatter.print(DateTime.now)))
  }

  private def emptyResult =
    (TokenRange(1, 0), lastRecord) // start is after end

  private def updateContext(values: Map[String, String] = Map(), emptyContextToUpdate: Boolean = false): Map[String, String] = {
    contextDataToUpdate synchronized {
      if (values.isEmpty) {
        val out = contextDataToUpdate
        if (emptyContextToUpdate) {
          contextDataToUpdate = Map.empty
        }
        out
      } else {
        val updated = contextDataToUpdate ++ values
        contextDataToUpdate = updated
        updated
      }
    }
  }

  def start(): Unit = {
    if (!hasNext)  {
      updateContext(Map(startDate -> dateFormatter.print(DateTime.now)))
      currentId += 1
    }
  }

  def stop(): Unit = {
    setDone()
    stoppedRequested = true
  }

  def getCurrentId: String = name + currentId

  abstract override def toContextData(data: Feeder.FeederData): TaskContext.ContextData = super.toContextData(data) ++
    Map(startDate -> getStartDate, finishedDate -> getFinishedDate)

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord]) = {
    context.data ++= updateContext(emptyContextToUpdate = true)
    if (stoppedRequested) {
      lastRecord = None
      currentRange = None
      stoppedRequested = false
    }
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
        case None if hasNext && !updateContext().contains(finishedDate) => (tokenRanges.head, None)
        case None => emptyResult
      }
    } else emptyResult
  }
}

object TableOnceFeeder {
  private val startDate = "start-date"
  private val finishedDate = "finished-date"
}
