package cn.pandadb.dbms

import com.typesafe.scalalogging.LazyLogging
import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService}
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
  val dataPath = config.getLocalDataStorePath()

  def getDbHome: String = {
    dataPath
  }

  override def getDatabase(name: String): GraphService = {
    defaultDB
  }

  override def createDatabase(name: String): GraphService = {
    val nodeMetaDBPath = config.getNodeMetaDBPath()
    val nodeDBPath = config.getNodeDBPath()
    val nodeLabelDBPath = config.getNodeLabelDBPath()
    val relationMetaDBPath = config.getRelationMetaDBPath()
    val relationDBPath = config.getRelationDBPath()
    val inRelationDBPath = config.getInRelationDBPath()
    val outRelationDBPath = config.getOutRelationDBPath()
    val relationLabelDBPath = config.getRelationLabelDBPath()
    val indexMetaDBPath = config.getIndexMetaDBPath()
    val indexDBPath = config.getIndexDBPath()
    val fulltextIndexPath = config.getFullIndexDBPath()
    val statisticsDBPath = config.getStatisticsDBPath()
    val rocksDBConfigFilePath: String = config.getRocksdbConfigFilePath()

    logger.info(s"nodeMetaDBPath  $nodeMetaDBPath ")
    logger.info(s"nodeDBPath  $nodeDBPath ")
    logger.info(s"nodeLabelDBPath  $nodeLabelDBPath ")
    logger.info(s"relationMetaDBPath  $relationMetaDBPath ")
    logger.info(s"relationDBPath  $relationDBPath ")
    logger.info(s"inRelationDBPath  $inRelationDBPath ")
    logger.info(s"outRelationDBPath  $outRelationDBPath ")
    logger.info(s"relationLabelDBPath  $relationLabelDBPath ")
    logger.info(s"indexMetaDBPath  $indexMetaDBPath ")
    logger.info(s"indexDBPath  $indexDBPath ")
    logger.info(s"fulltextIndexPath  $fulltextIndexPath ")
    logger.info(s"statisticsDBPath  $statisticsDBPath ")
    logger.info(s"rocksDBConfigFilePath $rocksDBConfigFilePath")
    GraphDatabaseBuilder.newEmbeddedDatabase2( nodeMetaDBPath,
      nodeDBPath,
      nodeLabelDBPath,
      relationMetaDBPath,
      relationDBPath,
      inRelationDBPath,
      outRelationDBPath,
      relationLabelDBPath,
      indexMetaDBPath,
      indexDBPath,
      fulltextIndexPath,
      statisticsDBPath,
      rocksDBConfigPath = rocksDBConfigFilePath)
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

}
