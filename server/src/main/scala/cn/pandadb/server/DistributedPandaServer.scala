package cn.pandadb.server

import cn.pandadb.dbms.DistributedGraphDatabaseManager
import cn.pandadb.kernel.udp.{UDPClient, UDPServer}
import cn.pandadb.kernel.udp.UDPServer
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.DistributedPandaRpcServer
import cn.pandadb.utils.ServerCommonUtils
import com.typesafe.scalalogging.LazyLogging


class DistributedPandaServer(config: Config) extends LazyLogging {
  var pandaRpcServer: DistributedPandaRpcServer = _

  val life = new LifecycleSupport

  val udpClients = ServerCommonUtils.getOtherClusterNodesIP(config).map(ip => new UDPClient(ip._1, ip._2))

  val database = new DistributedGraphDatabaseManager(config.getKVHosts(), config.getIndexHosts(), udpClients)

  pandaRpcServer = new DistributedPandaRpcServer(config, database)

  val udpServer = new UDPServer(config.getUDPPort(), database.defaultDB)

  life.add(database)
  life.add(pandaRpcServer)

  def start(): Unit = {
    udpServer.start()
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

