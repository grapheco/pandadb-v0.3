package cn.pandadb.server

import java.io.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils

import cn.pandadb.server.common.configuration.{Config, SettingKeys}


class PandaServerBootstrapper() extends LazyLogging {

  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");

  private var pandaServer: PandaServer = null
  def start(configFile: File, configOverrides: Map[String, String] = Map()): Unit = {
    addShutdownHook()
    val config = new Config().withFile(Option(configFile)).withSettings(configOverrides)
    println(logo)
    logger.info("==== PandaDB Server Starting... ====")

    config.getRocksdbConfigFilePath match {
      case "default" => logger.info("==== Using default RocksDB settings ====")
      case _ => logger.info("=== Using RocksDB configuration file ====")
    }

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



object PandaServerEntryPoint extends LazyLogging {

  def main(args: Array[String]): Unit = {
    /*
      args(0): conf file path
      args(1): default path of db's data folder, if conf file not define.
     */
    if (args.length <= 1) sys.error("need conf file and db path")
    else if (args.length > 2) sys.error("too much command")

    val configFile = new File(args(0))
    val dbHome = Map[String, String](SettingKeys.defaultLocalDBHome -> args(1))

    if (configFile.exists() && configFile.isFile()) {
      val serverBootstrapper = new PandaServerBootstrapper()
      serverBootstrapper.start(configFile, dbHome)
    }
    else {
      sys.error("can not find <conf-file> \r\n")
    }
  }

}