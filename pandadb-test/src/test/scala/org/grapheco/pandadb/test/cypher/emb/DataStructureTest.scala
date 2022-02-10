package org.grapheco.pandadb.test.cypher.emb

import java.io.File

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.store.{PandaNode, PandaRelationship}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: support data structure of cypher
 *              2021-04-26: Int、Long、Boolean、String、Double、Float
 *                          Array[Int]、Array[Long]、Array[Boolean]
 *                          Array[String]、Array[Double]、Array[Float]
 *              future: Blob
 * @author: LiamGao
 * @create: 2021-04-26
 */
class DataStructureTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  var db: DistributedGraphFacade = _
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))


  @Before
  def init(): Unit ={
    db = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
    db.cleanDB()
  }
  @Test
  def testNode(): Unit ={
    db.cypher(
      """
        |create (n:person:people{
        |name:'alex',
        | money1:100,
        | money2:233.3,
        | flag:true,
        | money11:[11,22,33,44],
        | money22:[22.1, 33.2, 44.3],
        | flags:[true, true, false],
        | jobs:['teacher', 'singer', 'player'],
        | hybridArr:[1, 2.0, "3.0", true]
        | }) return n
        |""".stripMargin).show()

    val res = db.cypher("match (n) return n").records().next()("n").asInstanceOf[PandaNode]
    val props = res.props.map{ case (key, value) => (key.value, value)}

    Assert.assertEquals(100L, props("money1").value)
    Assert.assertEquals(233.3, props("money2").value)
    Assert.assertEquals(true, props("flag").value)
    Assert.assertEquals("alex", props("name").value)
    Assert.assertEquals(Set(11L,22L,33L,44L), props("money11").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(22.1, 33.2, 44.3), props("money22").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(true, true, false), props("flags").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set("teacher", "singer", "player"), props("jobs").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(1, 2.0, "3.0", true), props("hybridArr").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set("person", "people"), res.labels.map(_.value).toSet)
  }

  @Test
  def testRelationship(): Unit ={
    db.cypher(
      """
        |create (n:person1{name:'A'})
        |create (m:person2{name:'B'})
        |""".stripMargin)

    db.cypher(
      """
        |match (n:person1)
        |match (m:person2)
        |create (n)-[r:KNOW{
        |name:'alex',
        | money1:100,
        | money2:233.3,
        | flag:true,
        | money11:[11,22,33,44],
        | money22:[22.1, 33.2, 44.3],
        | flags:[true, true, false],
        | jobs:['teacher', 'singer', 'player'],
        | hybridArr:[1, 2.0, "3.0", true]
        | }]->(m)
        |return r, n, m
        |""".stripMargin).show()

    val res = db.cypher("match (n)-[r]->(m) return r").records().next()("r").asInstanceOf[PandaRelationship]
    val props = res.props.map{ case (key, value) => (key.value, value)}

    Assert.assertEquals(100L, props("money1").value)
    Assert.assertEquals(233.3, props("money2").value)
    Assert.assertEquals(true, props("flag").value)
    Assert.assertEquals("alex", props("name").value)
    Assert.assertEquals(Set(11L,22L,33L,44L), props("money11").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(22.1, 33.2, 44.3), props("money22").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(true, true, false), props("flags").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set("teacher", "singer", "player"), props("jobs").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(1, 2.0, "3.0", true), props("hybridArr").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
