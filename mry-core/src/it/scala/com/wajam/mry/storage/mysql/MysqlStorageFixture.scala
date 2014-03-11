package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.Storage
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.mry.execution.{ExecutionContext, Value, Transaction}
import com.wajam.mry.storage.mysql.cache.CachedMysqlStorage

trait MysqlStorageFixture {

  case class FixtureParam(mysqlStorage: MysqlStorage, storages: Map[String, Storage]) {
    def exec(cb: (Transaction => Unit),
             commit: Boolean = true,
             onTimestamp: Timestamp = Timestamp(System.currentTimeMillis(), 0)): Seq[Value] = {
      val context = new ExecutionContext(storages, Some(onTimestamp))

      try {
        val trx = new Transaction()
        cb(trx)
        trx.execute(context)

        context.returnValues
      } finally {
        if (commit)
          context.commit()
        else
          context.rollback()
      }
    }
  }

  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table1_1 = table1.addTable(new Table("table1_1"))
  val table1_1_1 = table1_1.addTable(new Table("table1_1_1"))
  val table2 = model.addTable(new Table("table2"))
  val table2_1 = table2.addTable(new Table("table2_1"))
  val table2_1_1 = table2_1.addTable(new Table("table2_1_1"))

  val storageConfig = MysqlStorageConfiguration("mysql", "localhost", "mry", "mry", "mry",
    gcTokenStep = TokenRange.MaxToken)

  var currentConsistentTimestamp: Timestamp = Long.MaxValue

  def createMysqlStorage: MysqlStorage = new MysqlStorage(storageConfig, garbageCollection = false)

  def createCachedMysqlStorage: MysqlStorage = {
    new MysqlStorage(storageConfig, garbageCollection = false) with CachedMysqlStorage
  }

  def withFixture(test: (FixtureParam) => Unit)(implicit createStorage: () => MysqlStorage = createMysqlStorage _) = {

    // create the fixture
    var mysqlStorage: MysqlStorage = createStorage()
    try {
      // Setup the fixture
      mysqlStorage.nuke()
      mysqlStorage.syncModel(model)
      mysqlStorage.start()
      mysqlStorage.setCurrentConsistentTimestamp((_) => currentConsistentTimestamp)
      val theFixture = FixtureParam(mysqlStorage, Map("mysql" -> mysqlStorage))

      // "loan" the fixture to the test
      test(theFixture)
    }
    finally {
      // clean up the fixture
      mysqlStorage.stop()
    }
  }

}