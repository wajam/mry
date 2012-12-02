package com.wajam.mry.storage.mysql

import com.wajam.mry.execution._
import com.wajam.mry.storage.Storage
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import com.wajam.scn.Timestamp


abstract class TestMysqlBase extends FunSuite with BeforeAndAfterEach {
  var mysqlStorage: MysqlStorage = null
  var storages: Map[String, Storage] = null
  val model = new Model
  val table1 = model.addTable(new Table("table1"))
  val table1_1 = table1.addTable(new Table("table1_1"))
  val table1_1_1 = table1_1.addTable(new Table("table1_1_1"))
  val table2 = model.addTable(new Table("table2"))
  val table2_1 = table2.addTable(new Table("table2_1"))
  val table2_1_1 = table2_1.addTable(new Table("table2_1_1"))

  override def beforeEach() {
    this.mysqlStorage = newStorageInstance()
  }

  def newStorageInstance() = {
    val storage = new MysqlStorage(MysqlStorageConfiguration("mysql", "localhost", "mry", "mry", "mry"), garbageCollection = false)
    storages = Map(("mysql" -> storage))

    storage.nuke()
    storage.syncModel(model)
    storage.start()

    storage
  }

  override protected def afterEach() {
    mysqlStorage.stop()
  }

  def exec(cb: (Transaction => Unit), commit: Boolean = true, onTimestamp: Timestamp = Timestamp.now): Seq[Value] = {
    val context = new ExecutionContext(storages, Some(onTimestamp))

    try {
      val transac = new Transaction()
      cb(transac)
      transac.execute(context)

      context.returnValues
    } finally {
      if (commit)
        context.commit()
      else
        context.rollback()
    }
  }

  def createTimestamp(time: Long) = new Timestamp {
    def value = time
  }

}
