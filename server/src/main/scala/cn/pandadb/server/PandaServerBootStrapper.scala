package cn.pandadb.server

import java.io.File

import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config

class PandaServerBootstrapper() {
  private var pandaServer: PandaServer = null
  def start(configFile: File, configOverrides: Map[String, String] = Map()): Unit = {
    addShutdownHook()
//    val configFile = Option(configFile)
    val config = new Config().withFile(Option(configFile)).withSettings(configOverrides)
    pandaServer = new PandaServer(config)
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
//    if (args.length == 0) {
//      sys.error(s"Usage:\r\n");
//      sys.error(s"PandaServerEntryPoint <conf-file>\r\n");
//    }
//    val configFile = new File(args(0))
//    if (configFile.exists() && configFile.isFile()) {
//      serverBootstrapper.start(Some(configFile))
//    }
//    else {
//      sys.error("can not find <conf-file> \r\n")
//    }
    if (args.length == 0) sys.error("need conf file path")
    else if (args.length > 1) sys.error("too much command")

    val configFile = new File(args(0))

    if (configFile.exists() && configFile.isFile()) {
      val serverBootstrapper = new PandaServerBootstrapper()
      serverBootstrapper.start(configFile)
    }
    else {
      sys.error("can not find <conf-file> \r\n")
    }
  }

}