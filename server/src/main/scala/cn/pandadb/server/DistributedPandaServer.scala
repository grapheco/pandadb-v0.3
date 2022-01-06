package cn.pandadb.server

import cn.pandadb.dbms.DistributedGraphDatabaseManager
import cn.pandadb.kernel.udp.{UDPClient, UDPServer}
import cn.pandadb.kernel.udp.UDPServer
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleSupport
import cn.pandadb.server.rpc.DistributedPandaRpcServer
import cn.pandadb.utils.ServerCommonUtils
import com.typesafe.scalalogging.LazyLogging


class DistributedPandaServer(config: Config) extends LazyLogging {
  var pandaRpcServer: DistributedPandaRpcServer = _
  var udpClients: Array[UDPClient] = _
  var udpServer: UDPServer = _
  var database: DistributedGraphDatabaseManager = _
  val life = new LifecycleSupport

  val ips = ServerCommonUtils.getClusterNodesIP(config)
  if (ips._1) { // local mode
    val _ips = ips._2
    var notUsePort: Int = 0
    _ips.foreach(ap => {
      if (!ServerCommonUtils.isUDPPortUsing(ap._2)) notUsePort = ap._2
    })
    if (notUsePort == 0) throw new PandaDBException("all panda nodes port are already in use...")
    else {
      val other = ips._2.filterNot(p => p._2 == notUsePort)
      udpClients = other.map(ip => new UDPClient(ip._1, ip._2))
      database = new DistributedGraphDatabaseManager(config.getKVHosts(), config.getIndexHosts(), udpClients)
      pandaRpcServer = new DistributedPandaRpcServer(config, database)
      udpServer = new UDPServer(notUsePort, database.defaultDB)
    }
  }
  else {
    val local = ips._2.head
    val other = ips._2.tail
    udpClients = other.map(ip => new UDPClient(ip._1, ip._2))
    database = new DistributedGraphDatabaseManager(config.getKVHosts(), config.getIndexHosts(), udpClients)
    pandaRpcServer = new DistributedPandaRpcServer(config, database)
    udpServer = new UDPServer(local._2, database.defaultDB)
  }

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

