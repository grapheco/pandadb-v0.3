package org.grapheco.pandadb.dbms

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import com.typesafe.scalalogging.LazyLogging
import org.grapheco.pandadb.server.common.lifecycle.LifecycleAdapter

class DistributedGraphDatabaseManager(kvHosts: String, indexHosts: String, udpClientManager: UDPClientManager) extends LifecycleAdapter with LazyLogging {

  val defaultDB: DistributedGraphFacade = new DistributedGraphFacade(kvHosts, indexHosts, udpClientManager)

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
