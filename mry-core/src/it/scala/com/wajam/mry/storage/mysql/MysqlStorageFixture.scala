package com.wajam.mry.storage.mysql

import com.wajam.mry.storage.Storage
import com.wajam.nrv.service.TokenRange
import com.wajam.nrv.utils.timestamp.Timestamp
import com.wajam.mry.execution.{ExecutionContext, Value, Transaction}

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

    def execDryMode(cb: (Transaction => Unit),
                    onTimestamp: Timestamp = Timestamp(System.currentTimeMillis(), 0)): Unit = {
      val context = new ExecutionContext(storages, Some(onTimestamp))
      context.dryMode = true

      val trx = new Transaction()
      cb(trx)
      trx.execute(context)
    }
  }

  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table1_1 = table1.addTable(new Table("table1_1"))
  val table1_1_1 = table1_1.addTable(new Table("table1_1_1"))
  val table2 = model.addTable(new Table("table2"))
  val table2_1 = table2.addTable(new Table("table2_1"))
  val table2_1_1 = table2_1.addTable(new Table("table2_1_1"))

  val defaultConfig = MysqlStorageConfiguration("mysql", "localhost", "mry", "mry", "mry",
    gcTokenStep = TokenRange.MaxToken)

  var currentConsistentTimestamp: Timestamp = Long.MaxValue

  def withFixture(test: (FixtureParam) => Unit)(implicit config: MysqlStorageConfiguration = defaultConfig) = {

    // create the fixture
    val mysqlStorage: MysqlStorage = new MysqlStorage(config, garbageCollection = false)
    try {
      // Setup the fixture
      mysqlStorage.nuke()
      mysqlStorage.syncModel(model)
      mysqlStorage.start()
      mysqlStorage.setCurrentConsistentTimestamp((_) => currentConsistentTimestamp)
      val fixture = FixtureParam(mysqlStorage, Map("mysql" -> mysqlStorage))

      // "loan" the fixture to the test
      test(fixture)
    }
    finally {
      // clean up the fixture
      mysqlStorage.stop()
    }
  }

}