package cn.pandadb.dbms

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.net.udp.UDPClient
import com.typesafe.scalalogging.LazyLogging
import cn.pandadb.server.common.lifecycle.LifecycleAdapter

class DistributedGraphDatabaseManager(kvHosts: String, indexHosts: String, udpClients: Array[UDPClient]) extends LifecycleAdapter with LazyLogging {

  val defaultDB: DistributedGraphFacade = new DistributedGraphFacade(kvHosts, indexHosts, udpClients)

  override def stop(): Unit = {
    defaultDB.close()
    logger.info("==== ...db closed... ====")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def init(): Unit = {
    logger.debug(s"${this.getClass} inited.")
  }

}
