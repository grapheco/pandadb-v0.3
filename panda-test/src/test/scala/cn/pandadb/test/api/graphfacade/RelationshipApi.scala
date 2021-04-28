package cn.pandadb.test.api.graphfacade

import java.io.File

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

import scala.io.Source

class RelationshipApi {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var db: GraphFacade = _

  val longTextFile = Source.fromFile(new File("./testdata/longText"))
  val longText = longTextFile.getLines().next()

  var nodeId1: Long = _
  var nodeId2: Long = _
  var nodeId3: Long = _

  @Before
  def init(): Unit = {
    val dbPath = "./testdata/testdb/test.db"

    FileUtils.deleteDirectory(new File("./testdata/testdb"))
    FileUtils.forceMkdir(new File(dbPath))

    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    db = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )

    nodeId1 = db.addNode(
      Map("string"->longText, "int"->2147483647, "long"->2147483648L, "double"->233.3, "boolean"->true,
        "intArray"->Array[Int](1,2,3),
        "stringArray"->Array[String]("aa", "bb", "cc"),
        "longArray"->Array[Long](2147483648L, 2147483649L, 21474836488L),
        "booleanArray"->Array[Boolean](true, false, true),
        "doubleArray"->Array[Double](1.1, 2.2, 55555555.5)))
    nodeId2 = db.addNode(Map(), "person")
    nodeId3 = db.addNode(Map(), "person", "singer", "fighter", "star")
  }

  @Test
  def testRelationship(): Unit ={
    val n1 = db.addNode(Map("name"->"Oliver Stone", "sex"->"male"), "Person","Director")
    val n2 = db.addNode(Map("name"->"Michael Douglas"), "Person")
    val n3 = db.addNode(Map("name"->"Charlie Sheen"), "Person")
    val n4 = db.addNode(Map("name"->"Martin Sheen"), "Person")
    val n5 = db.addNode(Map("name"->"Rob Reiner"), "Person", "Director")
    val m1 = db.addNode(Map("title"->"Wall Street", "year"->1987), "Movie")
    val m2 = db.addNode(Map("title"->"The American President"), "Movie")

    val directedR1 = db.addRelation("DIRECTED", n1, m1, Map())
    val directedR2 = db.addRelation("DIRECTED", n5, m2, Map())
    val actedR1 = db.addRelation("ACTED_IN", n2, m1, Map("role"->"Gordon Gekko"))
    val actedR2 = db.addRelation("ACTED_IN", n2, m2, Map("role"->"President Andrew Shepherd"))
    val actedR3 = db.addRelation("ACTED_IN", n3, m1, Map("role"->"Bud Fox"))
    val actedR4 = db.addRelation("ACTED_IN", n4, m1, Map("role"->"Carl Fox"))
    val actedR5 = db.addRelation("ACTED_IN", n4, m2, Map("role"->"A.J. MacInerney"))

    val rid = db.addRelation("SAME", m1, m2, Map())
    db.relationSetProperty(rid, "str", "alex")
    db.relationSetProperty(rid, "strArr", Array("haha", "heihei", "xixi"))
    db.relationSetProperty(rid, "int", 23333)
    db.relationSetProperty(rid, "intArr", Array(123,456,789))
    db.relationSetProperty(rid, "double", 2333.33)
    db.relationSetProperty(rid, "doubleArr", Array(122.1, 233.2, 333.4))
    db.relationSetProperty(rid, "boolean", true)
    db.relationSetProperty(rid, "booleanArr", Array(true, true, false))


    val res1 = db.relationAt(rid).get
    Assert.assertEquals("SAME", db.relationAt(rid).get.relationType.get)
    Assert.assertEquals("alex", res1.properties("str").value)
    Assert.assertEquals(23333L, res1.properties("int").value)
    Assert.assertEquals(2333.33, res1.properties("double").value)
    Assert.assertEquals(true, res1.properties("boolean").value)
    Assert.assertEquals(List("haha", "heihei", "xixi"), res1.properties("strArr").value.asInstanceOf[List[LynxValue]].map(f=>f.value))
    Assert.assertEquals(List(123L,456L,789L), res1.properties("intArr").value.asInstanceOf[List[LynxValue]].map(f=>f.value))
    Assert.assertEquals(List(122.1, 233.2, 333.4), res1.properties("doubleArr").value.asInstanceOf[List[LynxValue]].map(f=>f.value))
    Assert.assertEquals(List(true, true, false), res1.properties("booleanArr").value.asInstanceOf[List[LynxValue]].map(f=>f.value))

    db.relationRemoveProperty(rid, "str")
    db.relationRemoveProperty(rid, "str")

    Assert.assertEquals(false, db.relationAt(rid).get.properties.contains("str"))

    Assert.assertEquals(8, db.relationships().size)

  }

  @After
  def close(): Unit = {
    db.close()
  }
}
