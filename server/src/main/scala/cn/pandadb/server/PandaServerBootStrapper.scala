package cn.pandadb.server

import java.io.File

import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.FileBasedIdGen
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import org.apache.commons.io.FileUtils
//import org.apache.logging.log4j.scala.Logging

class PandaServerBootStrapper {
  private var pandaServer: PandaServer = null
  def start(configFile: Option[File] = None, configOverrides: Map[String, String] = Map()): Unit = {
    addShutdownHook()
    val config = new Config().withFile(configFile).withSettings(configOverrides)
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
    val serverBootstrapper = new PandaServerBootStrapper
    serverBootstrapper.start()
  }

}