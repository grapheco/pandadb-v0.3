package cn.pandadb.server

import cn.pandadb.dbms.{DefaultGraphDatabaseManager, DefaultTransactionGraphDatabaseManager, TransactionGraphDatabaseManager}
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.{PandaRpcServer, TransactionPandaRpcServer}
import com.typesafe.scalalogging.LazyLogging


class TransactionPandaServer(config: Config) extends LazyLogging {
  var pandaRpcServer: TransactionPandaRpcServer = _
  val life = new LifecycleSupport

  val graphDatabaseManager: TransactionGraphDatabaseManager =
    new DefaultTransactionGraphDatabaseManager(config)

  pandaRpcServer = new TransactionPandaRpcServer(config, graphDatabaseManager)

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

