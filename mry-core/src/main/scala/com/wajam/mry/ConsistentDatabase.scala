package com.wajam.mry

import api.TransactionPrinter
import com.wajam.nrv.consistency.ConsistentStore
import com.wajam.nrv.data.{MessageType, InMessage, Message}
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.nrv.service.{ActionMethod, TokenRange}
import execution.{ExecutionContext, Transaction}
import com.wajam.mry.storage.{Storage, ConsistentStorage}
import com.wajam.nrv.utils.Closable
import com.yammer.metrics.scala.Instrumented

/**
 * Consistent MRY database
 */
trait ConsistentDatabase extends ConsistentStore with Instrumented {

  self: Database =>

  lazy private val lastTimestampTimer = metrics.timer("get-last-timestamp")
  lazy private val truncateTransactionTimer = metrics.timer("truncate-transaction")
  lazy private val readTransactionsInitTimer = metrics.timer("read-transactions-init")
  lazy private val readTransactionsNextTimer = metrics.timer("read-transactions-next")
  lazy private val writeTransactionTimer = metrics.timer("write-transaction")

  def consistentStorages: Iterable[ConsistentStorage] = storages.values.collect {
    case storage: ConsistentStorage => storage
  }

  def requiresConsistency(message: Message): Boolean = {
    if (consistentStorages.nonEmpty) {
      findAction(message.path, message.method) match {
        case Some(action) => {
          action == remoteWriteExecuteToken || action == remoteReadExecuteToken
        }
        case _ => false
      }
    } else false
  }

  /**
   * Returns the latest record timestamp for the specified token ranges
   */
  def getLastTimestamp(ranges: Seq[TokenRange]): Option[Timestamp] = {
    lastTimestampTimer.time {
      consistentStorages.map(_.getLastTimestamp(ranges)).max
    }
  }

  /**
   * Setup the function which returns the most recent timestamp considered as consistent by the Consistency manager
   * for the specified token range. The consistency of the records more recent that the consistent timestamp is
   * unconfirmed and these records must be excluded from processing tasks such as GC or percolation.
   */
  def setCurrentConsistentTimestamp(getCurrentConsistentTimestamp: (TokenRange) => Timestamp) {
    for (storage <- consistentStorages) {
      storage.setCurrentConsistentTimestamp(getCurrentConsistentTimestamp)
    }
  }

  /**
   * Returns the mutation messages from the given timestamp inclusively for the specified token ranges.
   */
  def readTransactions(fromTime: Timestamp, toTime: Timestamp, ranges: Seq[TokenRange]): Iterator[Message] with Closable = {
    // TODO: somehow support more than one storage
    val storage = consistentStorages.head

    readTransactionsInitTimer.time {
      new Iterator[Message] with Closable {
        val itr = storage.readTransactions(fromTime, toTime, ranges)

        def hasNext = {
          try {
            itr.hasNext
          } catch {
            case e: Exception => {
              close()
              throw e
            }
          }
        }

        def next() = {
          readTransactionsNextTimer.time {
            try {
              val value = itr.next()
              val transaction = new Transaction()
              value.applyTo(transaction)
              val message = new InMessage(Map(Database.TOKEN_KEY -> value.token), data = transaction)
              message.token = value.token
              message.timestamp = Some(value.timestamp)
              message.serviceName = name
              message.method = ActionMethod.POST
              message.function = MessageType.FUNCTION_CALL
              message.path = remoteWriteExecuteToken.path.buildPath(message.parameters)
              message
            } catch {
              case e: Exception => {
                close()
                throw e
              }
            }
          }
        }

        def close() {
          itr.close()
        }
      }
    }
  }


  /**
   * Apply the specified mutation message to this consistent database
   */
  def writeTransaction(message: Message) {
    writeTransactionTimer.time {
      val transaction = message.getData[Transaction]
      val timestamp = message.timestamp
      val context = new ExecutionContext(storages, timestamp)
      context.cluster = cluster

      try {
        transaction.execute(context)
        context.commit()
        transaction.reset()
      } catch {
        case e: Exception => {
          if (log.isDebugEnabled) {
            val txTree = TransactionPrinter.printTree(transaction, timestamp.getOrElse("").toString)
            info("Got an exception executing transaction {}", txTree, e)
          }
          context.rollback()
          throw e
        }
      }
    }
  }

  /**
   * Truncate all records at the given timestamp for the specified token.
   */
  def truncateAt(timestamp: Timestamp, token: Long) {
    truncateTransactionTimer.time {
      for (storage <- consistentStorages) {
        storage.truncateAt(timestamp, token)
      }
    }
  }
}
