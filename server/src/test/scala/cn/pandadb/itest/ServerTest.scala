package cn.pandadb.itest

import java.io.File

import cn.pandadb.server.PandaServerBootstrapper
import cn.pandadb.server.common.configuration.{Config, SettingKeys}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Test}

import scala.collection.mutable

class ServerTest {

  val settings = mutable.HashMap(
    "dbms.server.rpc.listen.host" -> "0.0.0.0",
    "dbms.server.rpc.listen.port"->"9989",
    "dbms.server.rpc.service.name"->"pandadb-server"
  )

  @Test
  def testConfHasDBDescribe(): Unit ={
    settings += SettingKeys.localDBHomePath -> "./testdata/pandadb"
    settings += SettingKeys.localDBName -> "hasPandadb.db"
    settings += SettingKeys.defaultLocalDBHome -> "./testdata/pandadb_default"

    FileUtils.deleteDirectory(new File(settings(SettingKeys.localDBHomePath)))
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(null, settings.toMap)
  }

  @Test
  def testConfNoDBDescribe(): Unit ={
    settings += SettingKeys.defaultLocalDBHome -> "./testdata/pandadb_default"
    FileUtils.deleteDirectory(new File(settings(SettingKeys.defaultLocalDBHome)))
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(null, settings.toMap)
  }

  @Test
  def testConfStart(): Unit ={
    val confFile = new File("./testdata/pandadb.conf")
    FileUtils.deleteDirectory(new File("/testdata"))
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(confFile, Map("db.default.home.path" -> "./testdata"))
  }
}
