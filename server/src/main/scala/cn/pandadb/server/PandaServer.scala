package cn.pandadb.server

import cn.pandadb.dbms.GraphDatabaseManager
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport


class PandaServer(config: Config) extends Logging {

  val life = new LifecycleSupport

  val localStorePath = config.getLocalDataStorePath()

  val graphDatabaseManager: GraphDatabaseManager = null

//  life.add(new PandaRpcServer(config, graphDatabaseManager) )

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

