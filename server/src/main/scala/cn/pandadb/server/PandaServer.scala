package cn.pandadb.server

import java.io.FileInputStream

import cn.pandadb.dbms.{DefaultGraphDatabaseManager, GraphDatabaseManager}
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.PandaRpcServer


class PandaServer(config: Config) extends Logging {
  var pandaRpcServer: PandaRpcServer = _
  val life = new LifecycleSupport

  val localStorePath = config.getLocalDataStorePath()
  val graphDatabaseManager: DefaultGraphDatabaseManager =
    new DefaultGraphDatabaseManager(config.getLocalDataStorePath())

  pandaRpcServer = new PandaRpcServer(config, graphDatabaseManager)

  life.add(graphDatabaseManager)
  life.add(pandaRpcServer)

  def start(): Unit = {
    logger.info("==== PandaDB Server Starting... ====")
    life.start()
  }

  def shutdown(): Unit = {
    logger.info("==== PandaDB Server Shutting Down... ====")
    pandaRpcServer.stop()
    life.shutdown()
    logger.info("==== PandaDB Server is Shutdown ====")
  }

}

