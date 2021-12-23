package cn.pandadb.kv.distributed.index

import java.nio.ByteBuffer

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping}
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.grapheco.lynx.{LynxInteger, LynxString, NodeFilter}
import org.junit.{After, Assert, Before, Test}
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
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"


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
    cleanDB()
    client = new RestHighLevelClient(RestClient.builder(hosts: _*))
    graphFacade = new DistributedGraphFacade(kvHosts, indexHosts)
    indexStore = new PandaDistributedIndexStore(client, graphFacade.db, graphFacade.nodeStore, new DistributedStatistics(graphFacade.db))
    cleanIndex()
    addData()
  }

  def cleanDB(): Unit ={
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }
  def cleanIndex(): Unit ={
    indexStore.cleanIndexes(NameMapping.indexName)
  }

  def addData(): Unit ={
    graphFacade.addNode(Map("age"->18, "name"->"A1", "country"->"China"), "person", "man", "coder")
    graphFacade.addNode(Map("age"->19, "name"->"A2", "country"->"USA"), "person")
    graphFacade.addNode(Map("age"->20, "name"->"A3", "country"->"CANADA"), "person")
    graphFacade.addNode(Map("age"->17, "name"->"A4", "country"->"AUS"), "person", "man")
    graphFacade.addNode(Map("age"->18, "name"->"A5", "country"->"CAF"), "person", "man")
  }

  @Test
  def createSinglePropIndexTest(): Unit ={
    graphFacade.cypher("create index on: person(age)")
    Thread.sleep(2000)
    val iter = graphFacade.getNodesByIndex(NodeFilter(Seq("person"), Map("age"->LynxInteger(18))))
    Assert.assertEquals(2, iter.size)
  }

  @Test
  def createMultiPropsIndexTest(): Unit ={
    graphFacade.cypher("create index on: person(age, name)")
    Thread.sleep(2000)
    val iter = graphFacade.getNodesByIndex(NodeFilter(Seq("person"), Map("age"->LynxInteger(18),  "name"->LynxString("A5"))))
    Assert.assertEquals(1, iter.size)
  }

  @Test
  def updatePropIndexTest(): Unit ={
    graphFacade.cypher("create index on: person(age)")
    graphFacade.cypher("create index on: person(name)")
    Thread.sleep(2000)
    val iter = graphFacade.getNodesByIndex(NodeFilter(Seq("person"), Map("age"->LynxInteger(18),  "name"->LynxString("A5"))))
    Assert.assertEquals(1, iter.size)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
