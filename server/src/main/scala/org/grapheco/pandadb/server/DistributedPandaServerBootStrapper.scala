package org.grapheco.pandadb.server

import java.io.File
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils

import org.grapheco.pandadb.server.common.configuration.{Config, SettingKeys}


class DistributedPandaServerBootstrapper() extends LazyLogging {

  val logo = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("logo.txt"), "utf-8");

  private var pandaServer: DistributedPandaServer = _

  addShutdownHook()

  def start(configFile: File, configOverrides: Map[String, String] = Map()): Unit = {

    val config = new Config().withFile(Option(configFile)).withSettings(configOverrides)

    println(logo)
    logger.info("==== PandaDB Server Starting... ====")

    pandaServer = new DistributedPandaServer(config)
    pandaServer.start()
  }

  private def addShutdownHook(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = {
        logger.info("hook processed...")
        pandaServer.shutdown()
      }
    })
  }
}



object DistributedPandaServerEntryPoint extends LazyLogging {

  def main(args: Array[String]): Unit = {
    /*
      started by script of pandadb.sh
      args(0): pandadb.conf file path
     */
    if (args.length != 1) sys.error("need conf file only")

    val configFile = new File(args(0))

    if (configFile.exists() && configFile.isFile()) {
      val serverBootstrapper = new DistributedPandaServerBootstrapper()
      serverBootstrapper.start(configFile)
    }
    else {
      sys.error("can not find <conf-file> \r\n")
    }
  }

}