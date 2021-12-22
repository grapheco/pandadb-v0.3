package cn.pandadb.server

import cn.pandadb.dbms.{DistributedGraphDatabaseManager}
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.{DistributedPandaRpcServer}
import com.typesafe.scalalogging.LazyLogging


class DistributedPandaServer(config: Config) extends LazyLogging {
  var pandaRpcServer: DistributedPandaRpcServer = _

  val life = new LifecycleSupport

  val database = new DistributedGraphDatabaseManager(config.getKVHosts(), config.getIndexHosts())

  pandaRpcServer = new DistributedPandaRpcServer(config, database)

  life.add(database)
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

