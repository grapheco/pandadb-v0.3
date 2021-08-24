package cn.pandadb.dbms

import cn.pandadb.kernel.transaction.PandaTransactionManager
import com.typesafe.scalalogging.LazyLogging
import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService, TransactionGraphDatabaseBuilder, TransactionGraphService}
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.lifecycle.LifecycleAdapter


trait TransactionGraphDatabaseManager extends LifecycleAdapter {

  def getDatabase(): PandaTransactionManager;

  def createDatabase(name: String): PandaTransactionManager;

  def shutdownDatabase(name: String): Unit = {
    allDatabases().foreach(db => db.close())
  }

  def deleteDatabase(name: String): Unit;

  def allDatabases(): Iterable[PandaTransactionManager];
}


class DefaultTransactionGraphDatabaseManager(config: Config) extends TransactionGraphDatabaseManager with LazyLogging {
  var defaultDB: PandaTransactionManager = null
  val defaultDBName = config.getLocalDBName()
  val dataPath = config.getLocalDataStorePath()

  def getDbHome: String = {
    dataPath
  }

  override def getDatabase(): PandaTransactionManager = {
    defaultDB
  }

  override def createDatabase(name: String): PandaTransactionManager = {
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
    val pandaLog = config.getPandaLogPath()

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
    logger.info(s"pandaLog $pandaLog")

    TransactionGraphDatabaseBuilder.newEmbeddedDatabase( nodeMetaDBPath,
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
      rocksDBConfigPath = rocksDBConfigFilePath, pandaLog)
  }

  override def stop(): Unit = {
    allDatabases().foreach(db => db.close())
    logger.info("==== ...db closed... ====")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def deleteDatabase(name: String): Unit = ???

  override def allDatabases(): Iterable[PandaTransactionManager] = {
    if (defaultDB != null) {
      List(defaultDB)
    }
    else List[PandaTransactionManager]()
  }

  override def init(): Unit = {
    logger.info(s"==== Data path: $dataPath ====")
    logger.info(s"==== DB name: $defaultDBName ====")
    defaultDB = createDatabase(defaultDBName)
    logger.debug(s"${this.getClass} inited.")
  }

}
