package cn.pandadb.itest

import java.io.File

import cn.pandadb.server.PandaServerBootstrapper
import cn.pandadb.server.common.configuration
import cn.pandadb.server.common.configuration.{Config, SettingKeys}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Test}

class ServerTest {

  val configFile = new File("./pandadb.conf")

  @Test
  def testAddExtraSettings(): Unit ={
    val dataHome = Map[String, String]("db.default.home.path"->"/pandadb")
    val config = new Config().withFile(Option(configFile)).withSettings(dataHome)
    Assert.assertEquals("/pandadb", config.getDefaultDBHome())
  }

  @Test
  def testConfHasDBDescribe(): Unit ={
    val map = Map[String, String](
      SettingKeys.localDBHomePath -> "/pandadb",
      SettingKeys.localDBName -> "hasPandadb.db",
      SettingKeys.defaultLocalDBHome -> "/pandadb_default"
    )

    FileUtils.deleteDirectory(new File(map.get(SettingKeys.localDBHomePath).get))
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(configFile, map)
  }

  @Test
  def testConfNoDBDescribe(): Unit ={
    val map = Map[String, String](
      SettingKeys.defaultLocalDBHome -> "/pandadb_default"
    )

    FileUtils.deleteDirectory(new File(map.get(SettingKeys.defaultLocalDBHome).get))
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(configFile, map)
  }
}
