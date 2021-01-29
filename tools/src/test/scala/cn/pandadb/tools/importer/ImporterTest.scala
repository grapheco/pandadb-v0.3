package cn.pandadb.tools.importer

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import org.junit.Test

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 21:29 2021/1/15
  * @Modified By:
  */
class ImporterTest {

  @Test
  def test1(): Unit ={
    val dBPath = ""
    val nodeAPI = new NodeStoreAPI(dBPath)
    val relationAPI = new RelationStoreAPI(dBPath)
    val node = nodeAPI.getNodeById(519791209300010L)
    val relation = relationAPI.getRelationById(15133L)
    val relation2 = relationAPI.getRelationById(5167L)
  }
}
