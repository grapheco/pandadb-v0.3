package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import cn.pandadb.net.udp.UDPClient
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: ${description}
 * @author: LiamGao
 * @create: 2021-05-19 11:06
 */
class SetTest {
  val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  var db: DistributedGraphFacade = _
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))

  @Before
  def init(): Unit ={
    db = new DistributedGraphFacade(kvHosts, indexHosts, udpClient)
    db.cleanDB()
  }

  @Test
  def testSetNodeProperty(): Unit ={
    db.cypher("create (n:person)")

    var res = db.cypher("match (n) set n.value='alex' return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue("alex"), res)

    res = db.cypher("match (n) set n.value=100 return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(100), res)

    res = db.cypher("match (n) set n.value=233.3 return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(233.3), res)

    res = db.cypher("match (n) set n.value=true return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(true), res)

    res = db.cypher("match (n) set n.value=[11,22,33,44] return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(List(LynxValue(11), LynxValue(22),LynxValue(33),LynxValue(44))), res)

    res = db.cypher("match (n) set n.value=[22.1, 33.2, 44.3] return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(List(LynxValue(22.1), LynxValue(33.2),LynxValue(44.3))), res)

    res = db.cypher("match (n) set n.value=['teacher', 'singer', 'player'] return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(List(LynxValue("teacher"), LynxValue("singer"),LynxValue("player"))), res)

    res = db.cypher("match (n) set n.value=[1, 2.0, '3.0', true] return n.value").records().next()("n.value")
    Assert.assertEquals(LynxValue(List(LynxValue(1), LynxValue(2.0),LynxValue("3.0"),LynxValue(true))), res)

    // type 2: set xxx= set yyy=
    var dataMap = db.cypher("match (n) set n.value1=1 set n.value2='a' return n").records().next()("n").asInstanceOf[PandaNode].properties
    Assert.assertEquals(LynxValue(1), dataMap("value1"))
    Assert.assertEquals(LynxValue("a"), dataMap("value2"))

    // type 3: set xxx=, yyy=
    dataMap = db.cypher("match (n) set n.value1=2 , n.value2='b' return n").records().next()("n").asInstanceOf[PandaNode].properties
    Assert.assertEquals(LynxValue(2), dataMap("value1"))
    Assert.assertEquals(LynxValue("b"), dataMap("value2"))

    // type 4: set +={}
    dataMap = db.cypher("match (n) set n +={value1:3, value2:'c'} return n").records().next()("n").asInstanceOf[PandaNode].properties
    Assert.assertEquals(LynxValue(3), dataMap("value1"))
    Assert.assertEquals(LynxValue("c"), dataMap("value2"))
  }

  @Test
  def testSetNodeLabels(): Unit ={
    db.cypher("create (n:person)")
    var res = db.cypher("match (n) set n:AAA return n").records().next()("n").asInstanceOf[PandaNode].labels
    Assert.assertEquals(Seq("person", "AAA"), res)

    res = db.cypher("match (n) set n:BBB:CCC return n").records().next()("n").asInstanceOf[PandaNode].labels
    Assert.assertEquals(Seq("person", "AAA", "BBB", "CCC"), res)
  }

  @Test
  def testSetRelationProperty(): Unit ={
    val n1 = db.addNode(Map("value"->"A"), "Person")
    val n2 = db.addNode(Map("value"->"B"), "Person")
    val r1 = db.addRelation("KNOW", n1, n2, Map("value"->"alex"))

    var res = db.cypher("match (n)-[r]->(m) set r.value='alex' return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue("alex"), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=100 return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(100), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=233.3 return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(233.3), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=true return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(true), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=[11,22,33,44] return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(List(LynxValue(11), LynxValue(22),LynxValue(33),LynxValue(44))), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=[22.1, 33.2, 44.3] return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(List(LynxValue(22.1), LynxValue(33.2),LynxValue(44.3))), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=['teacher', 'singer', 'player'] return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(List(LynxValue("teacher"), LynxValue("singer"),LynxValue("player"))), res)

    res = db.cypher("match (n)-[r]->(m) set r.value=[1, 2.0, '3.0', true] return r.value").records().next()("r.value")
    Assert.assertEquals(LynxValue(List(LynxValue(1), LynxValue(2.0),LynxValue("3.0"),LynxValue(true))), res)

    // type 2: set xxx= set yyy=
    var dataMap = db.cypher("match (n)-[r]->(m) set r.value1=1 set r.value2='a' return r").records().next()("r").asInstanceOf[PandaRelationship].properties
    Assert.assertEquals(LynxValue(1), dataMap("value1"))
    Assert.assertEquals(LynxValue("a"), dataMap("value2"))

    // type 3: set xxx=, yyy=
    dataMap = db.cypher("match (n)-[r]->(m) set r.value1=2, r.value2='b' return r").records().next()("r").asInstanceOf[PandaRelationship].properties
    Assert.assertEquals(LynxValue(2), dataMap("value1"))
    Assert.assertEquals(LynxValue("b"), dataMap("value2"))

    // type 4: set +={}
    dataMap = db.cypher("match (n)-[r]->(m) set r +={value1:3, value2:'c'} return r").records().next()("r").asInstanceOf[PandaRelationship].properties
    Assert.assertEquals(LynxValue(3), dataMap("value1"))
    Assert.assertEquals(LynxValue("c"), dataMap("value2"))
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
