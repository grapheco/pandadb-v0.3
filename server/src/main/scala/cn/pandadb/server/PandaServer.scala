package cn.pandadb.server

import java.io.FileInputStream

import cn.pandadb.dbms.{DefaultGraphDatabaseManager, GraphDatabaseManager}
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.PandaRpcServer


class PandaServer(config: Config) extends Logging {

  val life = new LifecycleSupport

  val localStorePath = config.getLocalDataStorePath()

  val graphDatabaseManager: DefaultGraphDatabaseManager =
    new DefaultGraphDatabaseManager(config.getLocalDataStorePath())
  life.add(graphDatabaseManager)
  life.add(new PandaRpcServer(config, graphDatabaseManager) )

  def start(): Unit = {
    logger.info("==== PandaDB Server Starting... ====")
    life.start()
    logger.info("==== PandaDB Server is Started ====")
  }

  def shutdown(): Unit = {
    logger.info("==== PandaDB Server Shutting Down... ====")
    life.shutdown()
    logger.info("==== PandaDB Server is Shutdown ====")
  }

}

