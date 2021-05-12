package cn.pandadb.kernel


import java.io.File

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import com.typesafe.scalalogging.StrictLogging



object GraphDatabaseBuilder extends StrictLogging{

  val subDirs = Set("auth", "index", "indexId", "indexMeta", "nodeLabel", "nodeMeta", "nodes",
    "inEdge", "outEdge", "relationMeta", "relLabelIndex", "rels", "statistics", "metadata")

  def newEmbeddedDatabase(dataPath: String, rocksdbConfPath: String = "default"): GraphService = {
    val file = new File(dataPath)
    if (!file.exists()) {
      file.mkdirs()
      logger.info(s"New created data path (${dataPath})")
    }
    else {
      if (file.isFile) {
        throw new Exception(s"The data path (${dataPath}) is invalid: not directory")
      }
    }

    val nodeStore = new NodeStoreAPI(dataPath, rocksdbConfPath)
    val relationStore = new RelationStoreAPI(dataPath, rocksdbConfPath)
    val indexStore = new IndexStoreAPI(dataPath, rocksdbConfPath)
    val statistics = new Statistics(dataPath, rocksdbConfPath)
    new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  def newEmbeddedDatabase2(nodeMetaDBPath: String,
                           nodeDBPath: String,
                           nodeLabelDBPath: String,
                           relationMetaDBPath: String,
                           relationDBPath: String,
                           inRelationDBPath: String,
                           outRelationDBPath: String,
                           relationLabelDBPath: String,
                           indexMetaDBPath: String,
                           indexDBPath: String,
                           fulltextIndexPath: String,
                           statisticsDBPath: String,
                           rocksDBConfigPath: String = "default"): GraphService = {

    checkDir(nodeMetaDBPath)
    checkDir(nodeDBPath)
    checkDir(nodeLabelDBPath)
    checkDir(relationMetaDBPath)
    checkDir(relationDBPath)
    checkDir(inRelationDBPath)
    checkDir(outRelationDBPath)
    checkDir(relationLabelDBPath)
    checkDir(indexMetaDBPath)
    checkDir(indexDBPath)
    checkDir(fulltextIndexPath)
    checkDir(statisticsDBPath)

    val nodeStore = new NodeStoreAPI( nodeDBPath, rocksDBConfigPath,
                                      nodeLabelDBPath, rocksDBConfigPath,
                                      nodeMetaDBPath, rocksDBConfigPath)
    val relationStore = new RelationStoreAPI( relationDBPath, rocksDBConfigPath,
                                              inRelationDBPath, rocksDBConfigPath,
                                              outRelationDBPath, rocksDBConfigPath,
                                              relationLabelDBPath, rocksDBConfigPath,
                                              relationMetaDBPath, rocksDBConfigPath)
    val indexStore = new IndexStoreAPI( indexMetaDBPath, rocksDBConfigPath,
                                        indexDBPath, rocksDBConfigPath, fulltextIndexPath: String)
    val statistics = new Statistics( statisticsDBPath, rocksDBConfigPath)
    new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  private def checkDir(dir: String): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      file.mkdirs()
      logger.info(s"New created data path (${dir})")
    }
    else {
      if (!file.isDirectory) {
        throw new Exception(s"The data path (${dir}) is invalid: not directory")
      }
    }
  }

  private def assureFileExist(path: String): Unit = {
    val file = new File(path)
    if (!file.exists()) {
      throw new Exception(s"Can not find file (${path}) !")
    }
    else if(!file.isFile) {
      throw new Exception(s"The file (${path}) is invalid: not file")
    }
  }
}
