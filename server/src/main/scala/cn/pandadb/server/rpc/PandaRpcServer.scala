package cn.pandadb.server.rpc

import cn.pandadb.dbms.GraphDatabaseManager
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.modules.LifecycleServerModule

class PandaRpcServer(config: Config, dbManager: GraphDatabaseManager)
  extends LifecycleServerModule with Logging{

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")

    val graphService = dbManager.getDatabase("default")

    logger.info("default database: " + graphService.toString)

  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")

  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }


}
