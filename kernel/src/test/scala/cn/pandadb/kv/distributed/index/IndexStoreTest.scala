package cn.pandadb.kv.distributed.index

import cn.pandadb.kernel.distribute.index.{DistributedIndexStore, PandaDistributedIndexStore}
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.junit.{Before, Test}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 14:36
 */
class IndexStoreTest {
  val hosts = Array(new HttpHost("10.0.82.144", 9200, "http"),
    new HttpHost("10.0.82.145", 9200, "http"),
    new HttpHost("10.0.82.146", 9200, "http"))

  var client: RestHighLevelClient = _
  var indexStore: DistributedIndexStore = _

  val nodeIndex = "node-meta"
  val relationIndex = "relation-meta"
  val propertyIndex = "property-meta"

  @Before
  def init(): Unit = {
    client = new RestHighLevelClient(RestClient.builder(hosts: _*))
    indexStore = new PandaDistributedIndexStore(client)
  }

  @Test
  def cleanIndex(): Unit ={
    indexStore.cleanIndexes(nodeIndex, relationIndex, propertyIndex)
  }

  @Test
  def createIndexTest(): Unit ={
    indexStore.createIndex(nodeIndex, Map("refresh_interval" -> "1s"))
    indexStore.createIndex(relationIndex, Map("refresh_interval" -> "1s"))
    indexStore.createIndex(propertyIndex, Map("refresh_interval" -> "1s"))
  }

  @Test
  def deleteIndexes(): Unit ={
    indexStore.deleteIndex(nodeIndex)
    indexStore.deleteIndex(relationIndex)
    indexStore.deleteIndex(propertyIndex)
  }

  @Test
  def addMeta(): Unit ={
    indexStore.addMetaDoc(nodeIndex, "person1", 233) // code: 201 accept
    indexStore.addMetaDoc(nodeIndex, "person2", 234)
    indexStore.addMetaDoc(nodeIndex, "person3", 235)
  }
  @Test
  def deleteMeta(): Unit ={
    indexStore.deleteMetaDoc(nodeIndex, 234)
  }

  @Test
  def searchMeta(): Unit ={
    val res = indexStore.searchDoc(nodeIndex, "person2")
    if (res.isDefined) println(res.get.toSeq)
  }

  @Test
  def loadALl(): Unit ={
    val res = indexStore.loadAllMeta(nodeIndex)
    println(res._1.toList)
    println(res._2.toList)
  }
}
