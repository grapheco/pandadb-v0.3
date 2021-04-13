package cn.pandadb.server

import com.typesafe.scalalogging.LazyLogging

import cn.pandadb.dbms.DefaultGraphDatabaseManager
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.PandaRpcServer


class PandaServer(config: Config) extends LazyLogging {
  var pandaRpcServer: PandaRpcServer = _
  val life = new LifecycleSupport

  val graphDatabaseManager: DefaultGraphDatabaseManager =
    new DefaultGraphDatabaseManager(config)

  pandaRpcServer = new PandaRpcServer(config, graphDatabaseManager)

  life.add(graphDatabaseManager)
  life.add(pandaRpcServer)

  def start(): Unit = {
    life.start()
  }

  def shutdown(): Unit = {
    logger.info("==== PandaDB Server Shutting Down... ====")
    pandaRpcServer.stop()
    logger.info("==== ...rpc stopped... ====")
    life.shutdown()
    logger.info("==== PandaDB Server is Shutdown ====")
  }

}

