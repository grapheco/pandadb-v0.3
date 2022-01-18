package org.grapheco.pandadb.test.cypher.emb

import java.io.File

import org.grapheco.pandadb.kernel.distribute.{DistributedGraphFacade, PandaDistributeKVAPI}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.lynx.{LynxNode, LynxValue}
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: supported cypher functions
 * @author: LiamGao
 * @create: 2021-04-26
 */
class FunctionTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  var db: DistributedGraphFacade = _
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))

  var nodeId1: Long = _
  var nodeId2: Long = _
  var nodeId3: Long = _

  @Before
  def init(): Unit ={
    db = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
    db.cleanDB()
    prepareData(db)
  }

  def prepareData(db: DistributedGraphFacade): Unit ={
    nodeId1 = db.addNode(Map("name"->"alex",
      "storage"->1000000,
      "salary"->2333.33,
      "isCoder"->true,
      "indexs"->Array[Int](1,2,3,4,5),
      "floats"->Array[Double](11.1, 22.2, 33.3),
      "bools"->Array[Boolean](true, true, false, false),
      "jobs"->Array[String]("teacher", "coder", "singer")), "person")

    nodeId2 = db.addNode(Map("name"->"bob",
      "storage"->1000000,
      "indexs"->Array[Int](1,2,3,4,5)), "people")

    nodeId3 = db.addNode(Map("name"->"clause",
      "storage"->1000000,
      "isCoder"->true,
      "indexs"->Array[Int](1,2,3,4,5)), "person")

    db.addRelation("KNOW", nodeId1, nodeId2, Map("year"->2021))
  }

  @Test
  def testCount(): Unit ={
    val res1 = db.cypher("match (n) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    val res2 = db.cypher("match (n:person) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value
    val res3 = db.cypher("match (n:people) return count(n)").records().next()("count(n)").asInstanceOf[LynxValue].value

    Assert.assertEquals(3L, res1)
    Assert.assertEquals(2L, res2)
    Assert.assertEquals(1L, res3)
  }

  @Test
  def testId(): Unit ={
    val res1 = db.cypher("match (n) return id(n)").records().toList.map(f => f("id(n)").asInstanceOf[LynxValue].value).toSet
    val res2 = db.cypher("match (n:person) return id(n)").records().toList.map(f => f("id(n)").asInstanceOf[LynxValue].value).toSet
    val res3 = db.cypher("match (n:people) return id(n)").records().toList.map(f => f("id(n)").asInstanceOf[LynxValue].value).toSet
    val res4 = db.cypher(s"match (n) where id(n)=$nodeId3 return n").records().next()("n").asInstanceOf[LynxNode]
    Assert.assertEquals(Set(nodeId1, nodeId2, nodeId3), res1)
    Assert.assertEquals(Set(nodeId1, nodeId3), res2)
    Assert.assertEquals(Set(nodeId2), res3)
    Assert.assertEquals(nodeId3, res4.id.value)
  }

  @Test
  def testType(): Unit ={
    val res1 = db.cypher("match (n)-[r]->(m) return type(r)").records().next()("type(r)").asInstanceOf[LynxValue].value
    Assert.assertEquals("KNOW", res1)
  }

  @Test
  def testExist(): Unit ={
    val res1 = db.cypher("match (n) where exists(n.name) return n").records().toList
    Assert.assertEquals(3, res1.size)

    val res2 = db.cypher("match (n) where exists(n.isCoder) return n").records().toList
    Assert.assertEquals(2, res2.size)
  }

  @Test
  def testToInteger(): Unit ={
    val res1 = db.cypher("match (n{name:'alex'}) return toInteger(n.salary)").records().next()("toInteger(n.salary)").asInstanceOf[LynxValue].value
    Assert.assertEquals(2333L, res1)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
