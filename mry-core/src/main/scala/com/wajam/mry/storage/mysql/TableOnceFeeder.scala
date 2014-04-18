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

  private def getStartDate = context.data.get(startDate).map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))
  private def getFinishedDate = context.data.get(finishedDate).map(str => dateFormatter.parseDateTime(str.asInstanceOf[String]))
  private def getJobId = context.data.get(finishedDate).map(_.asInstanceOf[String])

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
    updateContext(Map(finishedDate -> dateFormatter.print(DateTime.now)))
  }

  private def emptyResult =
    (TokenRange(1, 0), lastRecord) // start is after end

  private def updateContext(values: Map[String, String]): Unit = Lock.synchronized {
    val updated = contextDataToUpdate ++ values
    contextDataToUpdate = updated
    updated
  }


  private def getContextToUpdate(emptyContextToUpdate: Boolean = false): Map[String, String] = Lock.synchronized {
    val out = contextDataToUpdate
    if (emptyContextToUpdate) {
      contextDataToUpdate = Map.empty
    }
    out
  }

  def start(newJobId: String): Boolean = Lock.synchronized {
    if (!hasNext && !contextDataToUpdate.contains(startDate)) {
      updateContext(Map(startDate -> dateFormatter.print(DateTime.now), jobId -> newJobId))
      true
    } else false
  }

  def stop(): Boolean = Lock.synchronized {
    if (hasNext || !contextDataToUpdate.contains(finishedDate)) {
      setDone()
      lastRecord = None
      currentRange = None
      true
    } else false
  }

  def getCurrentId: String = Lock.synchronized(name + getJobId)

  def isStarted: Boolean = hasNext

  abstract override def toContextData(data: Feeder.FeederData): TaskContext.ContextData = super.toContextData(data) ++
    Map(startDate -> getStartDate, finishedDate -> getFinishedDate, jobId -> getJobId)

  private[mysql] def getLoadPosition: (TokenRange, Option[DataRecord]) = {
    context.data ++= getContextToUpdate(emptyContextToUpdate = true)
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
        case None if hasNext && !getContextToUpdate().contains(finishedDate) => (tokenRanges.head, None)
        case None => emptyResult
      }
    } else emptyResult
  }
}

object TableOnceFeeder {
  private val startDate = "start-date"
  private val finishedDate = "finished-date"
  private val jobId = "job-id"
}
