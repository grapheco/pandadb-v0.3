package org.grapheco.pandadb.test.cypher.emb

import java.io.File

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.store.PandaNode
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

/*
[0427]
 --cyphers--
CREATE(n)
CREATE(n:label)
CREATE(n:label{k:v})
CREATE(n:label{k:[v,v,v])
--property data types--
 string,long,double,string[],long[],double[]
*/

class CreateNodeTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  var db: DistributedGraphFacade = _
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))


  @Before
  def init(): Unit ={
    db = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
    db.cleanDB()
  }

  @After
  def close(): Unit ={
    db.close()
  }

  @Test
  def createNode(): Unit = {
    val cypher = "CREATE (n)"
    db.cypher(cypher)
    Assert.assertEquals(1, db.scanAllNodes().size)
  }

  @Test
  def createNodeWithLabel(): Unit = {
    val cypher = "CREATE (n:Person)"
    db.cypher(cypher).records()
    val nodes = db.scanAllNodes().toList
    Assert.assertEquals(1, nodes.size)
  }

  @Test
  def testNotUseShowCreateData(): Unit ={
    db.cypher("CREATE (n:person{name:'a'}) RETURN n")
    val record = db.cypher("match (n) return n").records().next()
    val property = record("n").asInstanceOf[PandaNode].props.map{ case (key, value) => (key.value, value)}
    Assert.assertEquals("a", property("name").value)
  }

  @Test
  def node_String_Int_float_boolean(): Unit ={
    val record = db.cypher("CREATE (Keanu:Person {name:'String_Int_float_boolean', born:1964, " +
      "money:100.55, animal:false}) return Keanu").records().next()
    val property = record("Keanu").asInstanceOf[PandaNode].props.map{ case (key, value) => (key.value, value)}

    Assert.assertEquals("String_Int_float_boolean", property("name").value)
    Assert.assertEquals(1964L, property("born").value)
    Assert.assertEquals(100.55, property("money").value)
    Assert.assertEquals(false, property("animal").value)
  }

  @Test
  def node_Array_String(): Unit ={
    val record = db.cypher("CREATE (Keanu:Person {name:'node_List', born:1964, arr1:['singer', 'ceo']}) RETURN Keanu").records().next()
    val arr1 = record("Keanu").asInstanceOf[PandaNode].props.map{ case (key, value) => (key.value, value)}.get("arr1")
      .get.value.asInstanceOf[List[LynxValue]].map(x=>x.value).toArray
    Assert.assertEquals(2, arr1.length)
    Assert.assertEquals("singer", arr1(0))
    Assert.assertEquals("ceo", arr1(1))
  }

  @Test
  def node_Array_Number(): Unit ={
    val record = db.cypher("CREATE (Keanu:Person {name:'node_List', born:1964, arr1:[198893982, 1.1]}) RETURN Keanu").records().next()
    val arr1 = record("Keanu").asInstanceOf[PandaNode].props.map{ case (key, value) => (key.value, value)}.get("arr1")
      .get.value.asInstanceOf[List[LynxValue]].map(x=>x.value).toArray
    Assert.assertEquals(2, arr1.length)
    Assert.assertEquals(198893982L, arr1(0))
    Assert.assertEquals(1.1, arr1(1))
  }

  @Test
  def node_Array_Boolean(): Unit ={
    val record = db.cypher("CREATE (Keanu:Person {name:'node_List', born:1964, arr1:[true, false]}) RETURN Keanu").records().next()
    val arr1 = record("Keanu").asInstanceOf[PandaNode].props.map{ case (key, value) => (key.value, value)}.get("arr1")
      .get.value.asInstanceOf[List[LynxValue]].map(x=>x.value).toArray
    Assert.assertEquals(2, arr1.length)
    Assert.assertEquals(true, arr1(0))
    Assert.assertEquals(false, arr1(1))
  }

  @Test
  def node_Create_Match(): Unit = {
    db.cypher("CREATE (Keanu:Person {name:'node_List', born:1964, arr1:[true, false]}) RETURN Keanu").show()
    val record = db.cypher("MATCH(n) return n").records().toList
    Assert.assertEquals(1, record.size)
    val n = record(0)("n").asInstanceOf[PandaNode]
    val arr1 = n.props.map{ case (key, value) => (key.value, value)}.get("arr1")
      .get.value.asInstanceOf[List[LynxValue]].map(x=>x.value).toArray
    Assert.assertEquals(2, arr1.length)
    Assert.assertEquals(true, arr1(0))
    Assert.assertEquals(false, arr1(1))
    Assert.assertEquals("Person", n.labels(0).value)
  }
}
