package cn.pandadb.dbms

import scala.reflect.io.Path
import com.typesafe.scalalogging.LazyLogging

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleAdapter


trait GraphDatabaseManager extends LifecycleAdapter {

  def getDatabase(name: String): GraphService;

  def createDatabase(name: String): GraphService;

  def shutdownDatabase(name: String): Unit = {
    allDatabases().foreach(db => db.close())
  }

  def deleteDatabase(name: String): Unit;

  def allDatabases(): Iterable[GraphService];
}


class DefaultGraphDatabaseManager(config: Config) extends GraphDatabaseManager with LazyLogging {
  var defaultDB: GraphService = null
  val defaultDBName = config.getLocalDBName()
  val dataPath = {
    if (config.getLocalDataStorePath() != "not setting") {
      config.getLocalDataStorePath()
    } else {
      config.getDefaultDBHome() + "/data"
    }
  }

  def getDbHome: String = {
    dataPath
  }

  override def getDatabase(name: String): GraphService = {
    defaultDB
  }

  override def createDatabase(name: String): GraphService = {
    val dbPath = Path(dataPath)./(name).toString()
    newGraphFacade(dbPath)
  }

  override def stop(): Unit = {
    allDatabases().foreach(db => db.close())
    logger.info("==== ...db closed... ====")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def deleteDatabase(name: String): Unit = ???

  override def allDatabases(): Iterable[GraphService] = {
    if (defaultDB != null) {
      List(defaultDB)
    }
    else List[GraphService]()
  }

  override def init(): Unit = {
    logger.info(s"==== Data path: $dataPath ====")
    logger.info(s"==== DB name: $defaultDBName ====")
    defaultDB = createDatabase(defaultDBName)
    logger.debug(s"${this.getClass} inited.")
  }

  private def newGraphFacade(path: String): GraphFacade = {
    logger.debug(s"start to get NodeStoreAPI.")
    val nodeStore = new NodeStoreAPI(path, config.getRocksdbConfigFilePath)
    logger.debug(s"start to get RelStoreAPI")
    val relationStore = new RelationStoreAPI(path, config.getRocksdbConfigFilePath)
    logger.debug(s"node and rel db got.")
    val indexStore = new IndexStoreAPI(path, config.getRocksdbConfigFilePath)
    logger.debug(s"index db got.")
    val statistics = new Statistics(path, config.getRocksdbConfigFilePath)
    logger.debug(s"statistics db got.")
    new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }
}
