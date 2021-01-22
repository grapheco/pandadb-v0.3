package cn.pandadb.itest

import java.io.File

import cn.pandadb.server.PandaServerBootstrapper
import org.apache.commons.io.FileUtils


object ServerEntryPointTest {
  val conf = "./pandadb.conf"
  val dbPath = "./testoutdb/"
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(dbPath))

    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(new File(conf))
  }

}
