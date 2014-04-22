package com.wajam.mry.storage.mysql

import com.github.nscala_time.time.Imports._
import org.joda.time.format.ISODateTimeFormat
import com.wajam.nrv.service.TokenRange
import com.wajam.spnl.feeder.Feeder
import com.wajam.spnl.TaskContext

/**
 * Feeder trait which iterate over table records sequentially per range of tokens. When started it resume from the last
 * acknowledged record saved in the task context. When it finish, it stops.
 */
trait TableOnceFeeder extends TableFeederByToken with Job {
  import TableOnceFeeder._

  private def getStartDate = context.data.get(StartDate).map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))
  private def getFinishedDate = context.data.get(FinishedDate).map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))
  private def getJobId = context.data.get(FinishedDate).map(_.asInstanceOf[String])

  private[this] object Lock
  private[this] var contextDataToUpdate = Map.empty[String, String]

  def dateFormatter = ISODateTimeFormat.dateTime()

  private[mysql] def hasNext: Boolean = Lock.synchronized {
    getStartDate match {
      case Some(date) => getFinishedDate.forall(_ < date) // Finished date, if defined, is older than start date
      case None => false
    }
  }

  private def setDone(): Unit = {
    updateContext(Map(FinishedDate -> dateFormatter.print(DateTime.now)))
  }

  private def emptyResult =
    (TokenRange(1, 0), lastRecord) // start is after end

  private def updateContext(values: Map[String, String]): Unit = Lock.synchronized {
    val updated = contextDataToUpdate ++ values
    contextDataToUpdate = updated
    updated
  }

  def start(newJobId: String): Boolean = Lock.synchronized {
    if (!hasNext && !contextDataToUpdate.contains(StartDate)) {
      updateContext(Map(StartDate -> dateFormatter.print(DateTime.now), JobId -> newJobId))
      true
    } else false
  }

  def stop(): Boolean = Lock.synchronized {
    if (hasNext || !contextDataToUpdate.contains(FinishedDate)) {
      setDone()
      lastRecord = None
      currentRange = None
      true
    } else false
  }

  def currentJobId: String = Lock.synchronized(name + getJobId)

  def isStarted: Boolean = hasNext

  abstract override def toContextData(data: Feeder.FeederData): TaskContext.ContextData = super.toContextData(data) ++
    Map(StartDate -> getStartDate, FinishedDate -> getFinishedDate, JobId -> getJobId)

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord]) = Lock.synchronized {
    context.data ++= contextDataToUpdate
    if (hasNext) {
      val loadRange = lastRecord match {
        case Some(record) => tokenRanges.find(token(record))
        case None => currentRange match {
          case Some(range) => tokenRanges.next(range) orElse {
            setDone()
            context.data ++= contextDataToUpdate
            None
          }
          case None => None
        }
      }

      contextDataToUpdate = Map.empty

      loadRange match {
        case Some(range) => (range, lastRecord)
        case None if hasNext => (tokenRanges.head, None)
        case None => emptyResult
      }
    } else emptyResult
  }
}

object TableOnceFeeder {
  private val StartDate = "start-date"
  private val FinishedDate = "finished-date"
  private val JobId = "job-id"
}
