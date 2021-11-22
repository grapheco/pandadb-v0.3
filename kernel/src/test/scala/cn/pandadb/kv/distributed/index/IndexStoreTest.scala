package cn.pandadb.kv.distributed.index

import cn.pandadb.kernel.distribute.index.{DistributedIndexStore, PandaDistributedIndexStore}
import cn.pandadb.kernel.distribute.meta.NameMapping
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

  val nodeMeta = NameMapping.nodeLabelMetaName
  val nodePropertyMeta = NameMapping.nodePropertyMetaName
  val nodeIndex = NameMapping.nodeIndex
  val nodeIndexMeta = NameMapping.nodeIndexMeta

  val relationMeta = NameMapping.relationTypeMetaName
  val relationPropertyMeta = NameMapping.relationPropertyMetaName
  val relationIndex = NameMapping.relationIndex
  val relationIndexMeta = NameMapping.relationIndexMeta

  val indexNames = Array(nodeMeta, nodePropertyMeta, nodeIndex, nodeIndexMeta, relationMeta,
    relationPropertyMeta, relationIndex, relationIndexMeta)

  @Before
  def init(): Unit = {
    client = new RestHighLevelClient(RestClient.builder(hosts: _*))
    indexStore = new PandaDistributedIndexStore(client)
  }

  @Test
  def cleanIndex(): Unit ={
    indexStore.cleanIndexes(indexNames:_*)
  }

  @Test
  def createIndexTest(): Unit ={
    indexStore.createIndex(nodeMeta, Map("refresh_interval" -> "1s"))
    indexStore.createIndex(relationMeta, Map("refresh_interval" -> "1s"))
    indexStore.createIndex(nodePropertyMeta, Map("refresh_interval" -> "1s"))
    indexStore.createIndex(relationPropertyMeta, Map("refresh_interval" -> "1s"))
  }

  @Test
  def deleteIndexes(): Unit ={
    indexStore.deleteIndex(nodeMeta)
    indexStore.deleteIndex(relationMeta)
    indexStore.deleteIndex(nodePropertyMeta)
    indexStore.deleteIndex(relationPropertyMeta)
  }

  @Test
  def addMeta(): Unit ={
    indexStore.addDoc(nodeMeta, "person1", 233) // code: 201 accept
    indexStore.addDoc(nodeMeta, "person2", 234)
    indexStore.addDoc(nodeMeta, "person3", 235)
  }
  @Test
  def deleteMeta(): Unit ={
    indexStore.deleteDoc(nodeMeta, 234)
  }

  @Test
  def searchMeta(): Unit ={
    val res = indexStore.searchDoc(nodeMeta, "person2")
    if (res.isDefined) println(res.get.toSeq)
  }

  @Test
  def loadALl(): Unit ={
    val res = indexStore.loadAllMeta(nodeMeta)
    println(res._1.toList)
    println(res._2.toList)
  }

  @Test
  def addIndexField: Unit ={
    val data = Iterator((1L, "China"), (2L, "USA"), (3L,"Canada"), (4L, "Japan"))
    indexStore.addIndexField("Country", data, nodeIndex)
  }
  @Test
  def updateIndexField: Unit ={
    val data = Iterator((1L, 5000), (2L, 200), (3L, 100), (4L, 100))
    indexStore.updateIndexField("History", data)

    val data2 = Iterator((1L, true), (2L, false), (3L, false), (4L, false))
    indexStore.updateIndexField("isStrong", data2)
  }
  @Test
  def deleteIndexField(): Unit ={
    indexStore.deleteIndexField("History")
  }
  @Test
  def addSingle(): Unit ={
    indexStore.addSingleIndexField(5.toString, "Country", "Korea")
  }
}
