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

  @Test
  def importData(): Unit = {
    val dbPath = "./src/test/output/testDB"
    val importCmd = s"./importer-panda.sh --db-path=$dbPath --nodes=./src/test/input/testdata.csv --delimeter=, --array-delimeter=|".split(" ")
    PandaImporter.main(importCmd)
    val nodeAPI = new NodeStoreAPI(dbPath)
    val node1 = nodeAPI.getNodeById(1L)
    val props = node1.get.properties
    println(props)
  }
}
