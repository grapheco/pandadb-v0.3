package cn.pandadb.itest

import cn.pandadb.server.PandaServerBootstrapper
import cn.pandadb.server.common.configuration.SettingKeys
import org.apache.commons.io.FileUtils

import java.io.File

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created in 15:46 2021/3/29
 * @Modified By:
 */
object StandAloneServer {
  def main(args: Array[String]): Unit = {
    val configFile = new File("./pandadb.conf")
    val map = Map[String, String](
      SettingKeys.localDBHomePath -> "pandadb",
//      SettingKeys.localDBName -> "hasPandadb.db",
      SettingKeys.defaultLocalDBHome -> "/pandadb_default"
    )

    FileUtils.deleteDirectory(new File(map.get(SettingKeys.localDBHomePath).get))
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(configFile, map)
  }

}
