package cn.pandadb.tools.importer

import java.io.File
import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.{GraphFacade, RocksDBStorage}
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Test}

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
    FileUtils.deleteDirectory(new File("./src/test/output/testDB") )
    val dbPath = "./src/test/output/testDB"
    val importCmd = s"./importer-panda.sh --db-path=$dbPath --nodes=./src/test/input/testdata.csv --delimeter=, --array-delimeter=|".split(" ")
    PandaImporter.main(importCmd)
    val nodeAPI = new NodeStoreAPI(dbPath)
    val node1 = nodeAPI.getNodeById(1L)
    val props = node1.get.properties
    println(props)
  }

  @Test
  def tmp1(): Unit ={
    val path = "/data/zzh/small.db"
//    val db = GraphDatabaseBuilder.newEmbeddedDatabase(path).asInstanceOf[GraphFacade]
    val nodeAPI = new NodeStoreAPI(path)

    val start1 = System.currentTimeMillis()
    val iter = nodeAPI.allNodes()
    val result1 = iter.toArray
    println(s"old way cost ${System.currentTimeMillis() - start1} ms") // s

    val start2 = System.currentTimeMillis()
    val iter2 = nodeAPI.all2()
    val result2 = iter2.toArray
    println(s"new way cost ${System.currentTimeMillis() - start2} ms") // s

    result1.zip(result2).map(pair => Assert.assertEquals(pair._1.id, pair._2.id))

  }
}
