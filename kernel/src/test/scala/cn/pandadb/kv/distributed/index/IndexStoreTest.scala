package cn.pandadb.kv.distributed.index

import java.nio.ByteBuffer

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping}
import cn.pandadb.kernel.kv.ByteUtils
import org.apache.http.HttpHost
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.script.Script
import org.grapheco.lynx.{LynxInteger, NodeFilter}
import org.junit.{After, Before, Test}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

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

  val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
  val session = TiSession.create(conf)
  val tikv: RawKVClient = session.createRawClient()

  var client: RestHighLevelClient = _
  var indexStore: PandaDistributedIndexStore = _
  var graphFacade: DistributedGraphFacade = _

  @Before
  def init(): Unit = {
//    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
//    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
//    tikv.deleteRange(left, right)

    client = new RestHighLevelClient(RestClient.builder(hosts: _*))
    graphFacade = new DistributedGraphFacade
    indexStore = new PandaDistributedIndexStore(client, graphFacade.db, graphFacade.nodeStore, new DistributedStatistics(graphFacade.db))

//    indexStore.deleteIndex(NameMapping.indexName)

  }


  def statistics(): Unit ={
    println("all node: ", graphFacade.statistics._allNodesCount)
    graphFacade.statistics._nodeCountByLabel.foreach(kv => println(s"labelId: ${kv._1}, count: ${kv._2}"))
    graphFacade.statistics._propertyCountByIndex.foreach(kv => println(s"indexed prop: ${kv._1}, count: ${kv._2}"))

    println("all relations: ", graphFacade.statistics._allRelationCount)
    graphFacade.statistics._relationCountByType.foreach(kv => println(s"typeId: ${kv._1}, count: ${kv._2}"))
  }

  @Test
  def currId(): Unit ={
    val res = tikv.get(ByteString.copyFrom(Array(19.toByte)))
    if (!res.isEmpty) println(ByteUtils.getLong(res.toByteArray, 0))
  }

  @Test
  def addNode(): Unit ={
    graphFacade.addNode(Map("age"->18, "name"->"glx1", "country"->"China"), "person", "man", "coder")
    graphFacade.addNode(Map("age"->19, "name"->"glx2", "country"->"USA"), "person")
    graphFacade.addNode(Map("age"->20, "name"->"glx3", "country"->"CANADA"), "person")
    graphFacade.addNode(Map("age"->17, "name"->"glx4", "country"->"AUS"), "person", "man")
    graphFacade.addNode(Map("age"->18, "name"->"glx5", "country"->"CAF"), "person", "man")
  }

  @Test
  def addNode2(): Unit ={
//    graphFacade.addNode(Map("age"->23333, "country"->"2233"), "person")
//    graphFacade.deleteNode(6)
    graphFacade.nodeSetProperty(7, "name", "2333")
  }

  @Test
  def createRelationship(): Unit ={
    graphFacade.addRelation("friend", 1, 2, Map.empty)
    graphFacade.addRelation("friend", 2, 3, Map.empty)
  }

  @Test
  def getNodes(): Unit ={
    graphFacade.scanAllNode().foreach(println)
  }
  @Test
  def getRels(): Unit ={
    graphFacade.cypher("match (n) return n").show()
//    graphFacade.cypher("match (n)-[r]->(n2)-[r2]->(m) return n,r,n2,r2,m").show()

    statistics()
  }

  @Test
  def cypher(): Unit ={
    graphFacade.cypher("match (n) return n").show()
  }

  @Test
  def createIndex(): Unit ={
//    graphFacade.cypher("create index on: person(age)")
    graphFacade.cypher("create index on: person(name)")
//    graphFacade.cypher("create index on: man(age)")
  }
  @Test
  def goIndex(): Unit ={
    graphFacade.getNodesByIndex(NodeFilter(Seq("person"), Map("age"->LynxInteger(18)))).foreach(println)
  }

  @Test
  def d(): Unit ={
    val script = s"ctx._source.remove('label')"
    val request = new UpdateRequest().index(NameMapping.indexName).id(1.toString).script(new Script(script))
    client.update(request, RequestOptions.DEFAULT)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
