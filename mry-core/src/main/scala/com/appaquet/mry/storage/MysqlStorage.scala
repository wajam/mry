package com.appaquet.mry.storage

import com.appaquet.mry.model.Model

/**
 * MySQL backed storage
 */
class MysqlStorage extends Storage {
  def init() = null

  def getTransaction = null

  def syncModel(model: Model) = null

  def nuke() = null
}
