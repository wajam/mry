package com.appaquet.mry.storage

import com.appaquet.mry.model.{Table, Model}
import com.appaquet.mry.execution.Timestamp

/**
 * In memory storage engine
 */
class MemoryStorage extends Storage {
  var globData = Map[(Table, Seq[String]), Record]()

  def getTransaction: StorageTransaction = new Transaction

  def syncModel(model: Model) {}

  def nuke() {}

  class Transaction extends StorageTransaction {
    var trxData = Map[(Table, Seq[String]), Option[Record]]()

    def set(table: Table, keys: Seq[String], record: Record) {
      this.trxData += ((table, keys) -> Some(record))
    }

    def get(table: Table, keys: Seq[String]): Option[Record] = {
      this.trxData.getOrElse((table, keys), globData.get((table, keys)))
    }

    def query(query: Query) = throw new Error("Implement me!")

    def timeline(table: Table, from: Timestamp) = throw new Error("Implement me!")

    def rollback() {}

    def commit() {
      for (((t, k), r) <- this.trxData) {
        r match {
          case Some(x) =>
            globData += ((t, k) -> x)
          case None =>
            globData -= ((t, k))
        }
      }
    }
  }

}
