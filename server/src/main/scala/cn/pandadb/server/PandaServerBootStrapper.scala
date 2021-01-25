package cn.pandadb.server

import java.io.File

import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import org.apache.commons.io.IOUtils

class PandaServerBootstrapper() {
  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");

  private var pandaServer: PandaServer = null
  def start(configFile: File, dataHome: String, configOverrides: Map[String, String] = Map()): Unit = {
    addShutdownHook()
    println(logo)

    val config = new Config().withFile(Option(configFile)).withSettings(configOverrides)
    pandaServer = new PandaServer(config, dataHome)
    pandaServer.start()
  }

  private def addShutdownHook(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        doShutDown()
      }
    })
  }

  def doShutDown(): Unit = {
    stop()
  }

  def stop(): Unit = {
    pandaServer.shutdown()
  }
}



object PandaServerEntryPoint extends Logging{

  def main(args: Array[String]): Unit = {
    if (args.length <= 1) sys.error("need conf file and db path")
    else if (args.length > 2) sys.error("too much command")

    val configFile = new File(args(0))
    val dataHome = args(1)
    if (configFile.exists() && configFile.isFile()) {
      val serverBootstrapper = new PandaServerBootstrapper()
      serverBootstrapper.start(configFile, dataHome)
    }
    else {
      sys.error("can not find <conf-file> \r\n")
    }
  }

}