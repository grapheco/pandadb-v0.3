package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.store.PandaRelationship
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxNode, LynxRelationship}
import org.junit.{After, Assert, Before, Test}


/*
[0427]
 --cyphers--
CREATE ()-[:type]->()
CREATE ()-[:type]->(),()-[:type]->()
CREATE ()-[:type]->()-[:type]->()
CREATE (m)-[:type]->(n)-[:type]->(t)
CREATE (m)-[:type]->(n)-[:type]->(m)
CREATE (m)-[r:type]->(n) RETURN r,m,n
 */

class CreateRelationTest {
  val dbPath = "./testdata/emb"
  var db: GraphFacade = _

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File(dbPath))
    FileUtils.forceMkdir(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
  }

  @After
  def close(): Unit ={
    db.close()
  }

  @Test
  def testRelation(): Unit = {
    val n1 = db.addNode(Map("name"->"A", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"B"), "Person")

    val cypher = """MATCH
                   |  (a:Person),
                   |  (b:Person)
                   |WHERE a.name = 'A' AND b.name = 'B'
                   |CREATE (a)-[r:RELTYPE]->(b)
                   |RETURN r""".stripMargin

    val res = db.cypher(cypher).records().toList
    Assert.assertEquals(1, res.size)
    val rel = res(0).get("r").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n1, rel.startNodeId.value)
    Assert.assertEquals(n2, rel.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel.relationType.get)
  }

  @Test
  def testMutliRelation(): Unit = {
    val n1 = db.addNode(Map("name"->"A", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"B"), "Person")
    val n3 = db.addNode(Map("name"->"C"), "Person")
    val n4 = db.addNode(Map("name"->"D"), "Person")

    val cypher = """MATCH
                   |  (a:Person{name:'A'}),
                   |  (b:Person{name:'B'}),
                   |  (c:Person{name:'C'}),
                   |  (d:Person{name:'D'})
                   |CREATE (a)-[r1:RELTYPE]->(b), (c)-[r2:RELTYPE]->(d)
                   |RETURN r1,r2""".stripMargin

    val res = db.cypher(cypher).records().toList
    Assert.assertEquals(1, res.size)
    val rel1 = res(0).get("r1").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n1, rel1.startNodeId.value)
    Assert.assertEquals(n2, rel1.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel1.relationType.get)
    val rel2 = res(0).get("r2").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n3, rel2.startNodeId.value)
    Assert.assertEquals(n4, rel2.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel2.relationType.get)
  }

  @Test
  def testTwoDegreeRelation(): Unit = {
    val n1 = db.addNode(Map("name"->"A", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"B"), "Person")
    val n3 = db.addNode(Map("name"->"C"), "Person")
    val n4 = db.addNode(Map("name"->"D"), "Person")

    val cypher = """MATCH
                   |  (a:Person{name:'A'}),
                   |  (b:Person{name:'B'}),
                   |  (c:Person{name:'C'}),
                   |  (d:Person{name:'D'})
                   |CREATE (a)-[r1:RELTYPE]->(b)-[r2:RELTYPE]->(c)
                   |RETURN r1,r2""".stripMargin

    val res = db.cypher(cypher).records().toList
    Assert.assertEquals(1, res.size)
    val rel1 = res(0).get("r1").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n1, rel1.startNodeId.value)
    Assert.assertEquals(n2, rel1.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel1.relationType.get)
    val rel2 = res(0).get("r2").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n2, rel2.startNodeId.value)
    Assert.assertEquals(n3, rel2.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel2.relationType.get)
  }

  @Test
  def testTwoDegreeRelation2(): Unit = {
    val n1 = db.addNode(Map("name"->"A", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"B"), "Person")
    val n3 = db.addNode(Map("name"->"C"), "Person")
    val n4 = db.addNode(Map("name"->"D"), "Person")

    val cypher = """MATCH
                   |  (a:Person{name:'A'}),
                   |  (b:Person{name:'B'}),
                   |  (c:Person{name:'C'}),
                   |  (d:Person{name:'D'})
                   |CREATE (a)-[r1:RELTYPE]->(d)-[r2:RELTYPE]->(a)
                   |RETURN r1,r2""".stripMargin

    val res = db.cypher(cypher).records().toList
    Assert.assertEquals(1, res.size)
    val rel1 = res(0).get("r1").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n1, rel1.startNodeId.value)
    Assert.assertEquals(n4, rel1.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel1.relationType.get)
    val rel2 = res(0).get("r2").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n4, rel2.startNodeId.value)
    Assert.assertEquals(n1, rel2.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel2.relationType.get)
  }

  @Test
  def testRelationWithReturn(): Unit = {
    val n1 = db.addNode(Map("name"->"A", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"B"), "Person")

    val cypher = """MATCH
                   |  (a:Person{name:'A'}),
                   |  (b:Person{name:'B'})
                   |CREATE (a)-[r1:RELTYPE{abc:[1,2,3]}]->(b)
                   |RETURN r1,a,b""".stripMargin

    val res = db.cypher(cypher).records().toList
    Assert.assertEquals(1, res.size)
    val rel1 = res(0).get("r1").get.asInstanceOf[LynxRelationship]
    Assert.assertEquals(n1, rel1.startNodeId.value)
    Assert.assertEquals(n2, rel1.endNodeId.value)
    Assert.assertEquals("RELTYPE", rel1.relationType.get)

    val nodeA = res(0).get("a").get.asInstanceOf[LynxNode]
    val nodeB = res(0).get("b").get.asInstanceOf[LynxNode]
    Assert.assertEquals(n1, nodeA.id.value)
    Assert.assertEquals(n2, nodeB.id.value)
  }


}
