package cn.pandadb.dbms

import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.lifecycle.{Lifecycle, LifecycleAdapter}

import scala.reflect.io.Path

trait GraphDatabaseManager extends LifecycleAdapter{

  def getDatabase(name: String): GraphService;

  def createDatabase(name: String): GraphService;

  def shutdownDatabase(name: String): Unit = {
    allDatabases().foreach(db=>db.close())
  }

  def deleteDatabase(name: String): Unit;

  def allDatabases(): Iterable[GraphService];
}


class DefaultGraphDatabaseManager(dataPath: String) extends GraphDatabaseManager with Logging{
  var defaultDB: GraphService = null
  val defaultDBName = "default"

  override def getDatabase(name: String): GraphService = {
    defaultDB
  }

  override def createDatabase(name: String): GraphService = {
    val dbPath = Path(dataPath)./(name).toString()
    newGraphFacade(dbPath)
  }

  override def stop(): Unit = {
    allDatabases().foreach(db => db.close())
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
    defaultDB = createDatabase(defaultDBName)
  }

  private def newGraphFacade(path: String): GraphFacadeWithPPD = {
    val nodeStore = new NodeStoreAPI(path)
    val relationStore = new RelationStoreAPI(path)
    val indexStore = new IndexStoreAPI(path)
    val statistics = new Statistics(path)
    new GraphFacadeWithPPD(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }
}
