package com.appaquet.mry.execution

import com.appaquet.mry.storage.Storage
import com.appaquet.mry.model.Model

/**
 * Execution context, used to store different information when a transaction
 * is executed.
 */
class ExecutionContext(var model:Model, var storage:Storage) {
  var timestamp = Timestamp.now
  var storageTransaction = storage.getTransaction(timestamp)
  var returnValues:Seq[Value] = Seq()
}
