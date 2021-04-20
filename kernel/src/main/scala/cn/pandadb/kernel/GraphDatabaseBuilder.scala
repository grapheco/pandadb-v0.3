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
//      file.list().foreach(f => if(!subDirs.contains(f)){
//        println(f)
//        throw new Exception(s"The data path (${dataPath}) is invalid: contains invalid files")
//      })
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

}
