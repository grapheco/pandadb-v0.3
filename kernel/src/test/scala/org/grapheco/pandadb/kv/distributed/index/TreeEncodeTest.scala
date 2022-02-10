package org.grapheco.pandadb.kv.distributed.index

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import org.grapheco.pandadb.kernel.distribute.index.encoding.{EncoderFactory, IndexEncoderNames}
import org.grapheco.pandadb.kernel.distribute.index.encoding.encoders.TreeEncoder
import org.grapheco.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.junit.{After, Before, Test}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-06 16:15
 */
class TreeEncodeTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))


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
  def init(): Unit ={
    client = new RestHighLevelClient(RestClient.builder(hosts: _*))
    graphFacade = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
    indexStore = new PandaDistributedIndexStore(client, graphFacade.db, graphFacade, new UDPClientManager(udpClient))

//    prepareData()
  }
  def prepareData(): Unit ={
    graphFacade.cleanDB
    indexStore.cleanIndexes(NameMapping.indexName)
    graphFacade.addNode(Map("name" -> "a1", "age" -> 11), "person", "worker")
    graphFacade.addNode(Map("name" -> "a2", "age" -> 12, "country" -> "China"), "person", "human")
    graphFacade.addNode(Map("name" -> "a3", "age" -> 13), "person", "CNIC")
    graphFacade.addNode(Map("name" -> "a4", "age" -> 12), "person", "worker")
    graphFacade.addNode(Map("name" -> "a5", "age" -> 15), "person", "man")

    graphFacade.addRelation("friend1", 1, 2, Map.empty)
    graphFacade.addRelation("friend2", 2, 3, Map.empty)
    graphFacade.addRelation("friend3", 4, 5, Map.empty)
    graphFacade.addRelation("friend4", 4, 2, Map("Year" -> 2020))
  }

  @Test
  def encode(): Unit ={
    val encoder = EncoderFactory.getEncoder(IndexEncoderNames.treeEncoder, indexStore).asInstanceOf[TreeEncoder]
    encoder.init("person", 1)
    encoder.encode(1, 2)
    encoder.encode(1, 3)
    encoder.encode(1, 4)
    encoder.encode(2, 5)
    encoder.close()
  }

  @Test
  def createIndex(): Unit ={
//    graphFacade.dropIndexOnNode("person", "age")
    graphFacade.createIndexOnNode("person", Set("name", "age"))
  }

  @Test
  def dropEncoder(): Unit ={
    val nodes = graphFacade.getNodesByLabel("person", false)
    indexStore.batchDropEncoder("person", "tree_encode", nodes)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
