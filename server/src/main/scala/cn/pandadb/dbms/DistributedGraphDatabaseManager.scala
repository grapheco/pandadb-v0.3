package cn.pandadb.dbms

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import com.typesafe.scalalogging.LazyLogging
import cn.pandadb.server.common.lifecycle.LifecycleAdapter

class DistributedGraphDatabaseManager(kvHosts: String, indexHosts: String) extends LifecycleAdapter with LazyLogging {

  var defaultDB: DistributedGraphFacade = _

  override def stop(): Unit = {
    defaultDB.close()
    logger.info("==== ...db closed... ====")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def init(): Unit = {
    defaultDB = new DistributedGraphFacade(kvHosts, indexHosts)
    logger.debug(s"${this.getClass} inited.")
  }

}
