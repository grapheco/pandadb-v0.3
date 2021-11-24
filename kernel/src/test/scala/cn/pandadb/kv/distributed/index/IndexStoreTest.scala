package cn.pandadb.kv.distributed.index

import cn.pandadb.kernel.distribute.index.{DistributedIndexStore, PandaDistributedIndexStore}
import cn.pandadb.kernel.distribute.meta.NameMapping
import org.apache.http.HttpHost
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.script.Script
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
  def addNameStoreMeta(): Unit ={
    indexStore.addNameMetaDoc(NameMapping.nodeLabelMetaName, "Teacher", 233) // code: 201 accept
    indexStore.addNameMetaDoc(NameMapping.nodeLabelMetaName, "Musician", 234)
    indexStore.addNameMetaDoc(NameMapping.nodeLabelMetaName, "People", 235)
  }

  @Test
  def searchNameMeta(): Unit ={
    println(indexStore.searchNameMetaDoc(NameMapping.nodeLabelMetaName, "People"))
    println(indexStore.searchNameMetaDoc(NameMapping.nodeLabelMetaName, 233))
  }
@Test
  def searchDoc(): Unit ={
  println(indexStore.docExist(NameMapping.nodeIndex, 233.toString))
}

  @Test
  def loadAllNameMeta(): Unit ={
    println(indexStore.loadAllMeta(NameMapping.nodeLabelMetaName))
  }

  @Test
  def addIndexField(): Unit ={
    indexStore.addIndexField(1, "Information", "Name", "glx", NameMapping.nodeIndex)
    indexStore.addIndexField(2, "Information", "Name", "bob", NameMapping.nodeIndex)
    indexStore.addIndexField(3, "Information", "Name", "Alice", NameMapping.nodeIndex)
  }
  @Test
  def updateField(): Unit ={
    indexStore.updateIndexField(1, "Information", "Age", 18, NameMapping.nodeIndex)
    indexStore.updateIndexField(2, "Information", "Age", 19, NameMapping.nodeIndex)
    indexStore.updateIndexField(3, "Information", "Age", 20, NameMapping.nodeIndex)

    indexStore.updateIndexField(1, "Information", "Year", 1888, NameMapping.nodeIndex)
    indexStore.updateIndexField(2, "Information", "Year", 1999, NameMapping.nodeIndex)
    indexStore.updateIndexField(3, "Information", "Year", 2000, NameMapping.nodeIndex)
  }
  @Test
  def deleteField(): Unit ={
    indexStore.deleteIndexField("Information", "Name", NameMapping.nodeIndex)
  }
  @Test
  def d(): Unit ={
    val script = s"ctx._source.remove('label')"
    val request = new UpdateRequest().index(NameMapping.nodeIndex).id(1.toString).script(new Script(script))
    client.update(request, RequestOptions.DEFAULT)
  }
}
