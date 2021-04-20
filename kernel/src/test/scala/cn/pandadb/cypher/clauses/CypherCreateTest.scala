package cn.pandadb.cypher.clauses

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, PandaNode, PandaRelationship, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxDate, LynxDateTime}
import org.junit.{After, Assert, Before, Test}

class CypherCreateTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  @Before
  def init(): Unit ={
    val dbPath = "./cypherTest.db"
    val file = new File(dbPath)
    if (file.exists()){
      FileUtils.deleteDirectory(file)
    }

    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  @Test
  def testNotUseShowCreateData(): Unit ={
    graphFacade.cypher("create (n:person{name:'a'}) return n")
    val record = graphFacade.cypher("match (n) return n").records().next()
    val property = record("n").asInstanceOf[PandaNode].properties
    Assert.assertEquals("a", property("name").value)
  }

  @Test
  def node_String_Int_float_boolean(): Unit ={
    val record = graphFacade.cypher("CREATE (Keanu:Person {name:'String_Int_float_boolean', born:1964, " +
      "money:100.55, animal:false}) return Keanu").records().next()
    val property = record("Keanu").asInstanceOf[PandaNode].properties

    Assert.assertEquals("String_Int_float_boolean", property("name").value)
    Assert.assertEquals(1964, property("born").value)
    Assert.assertEquals(100.55, property("money").value)
    Assert.assertEquals(false, property("animal").value)
  }

  @Test
  def node_Point1(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'Point1', born:1964, location:point({x:22.2, y:33.3})})").show()
  }
  @Test
  def node_Point2(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'Point2', born:1964, location:point({latitude: 1, longitude: 2})})").show()
  }

  @Test
  def node_List1(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_List', born:1964, job:['singer', 'ceo']})").show()
  }
  @Test
  def node_List2(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_List', born:1964, job:[1, 2, 3, 4]})").show()
  }
  @Test
  def node_Date1(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_Date1',born:date('2018-04-05')})").show()
    val res = graphFacade.cypher("MATCH (Keanu:Person {name:'node_Date1'}) RETURN Keanu.born").records()
    Assert.assertEquals(1522857600000L, res.next().get("Keanu.born").get.asInstanceOf[LynxDate].value)
  }
  @Test
  def node_Date2(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_Date2',born:date({year: 2020, month: 6, day: 6})})").show()
  }

  @Test
  def node_Date3(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_Date2',born:date()})").show()
  }

  @Test
  def node_DateTime1(): Unit = {
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_Date2',born:datetime()})").show()
  }

  @Test
  def node_DateTime2(): Unit = {
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_Date2',born:datetime('2018-02-03 12:09:11')})").show()
    val res = graphFacade.cypher("MATCH (Keanu:Person {name:'node_Date2'}) RETURN Keanu.born").records()
    Assert.assertEquals(1517630951000L, res.next().get("Keanu.born").get.asInstanceOf[LynxDateTime].value)
  }


  @Test
  def node_Localtime(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_Localtime',born:localtime('12:45:30.25')})").show()
  }
  @Test
  def node_time(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_time',born:time('12:45:30.25+01:00')})").show()
  }
  @Test
  def node_localdatetime(): Unit ={
    graphFacade.cypher("CREATE (Keanu:Person {name:'node_localdatetime',born:localdatetime('2018-04-05T12:34:00')})").show()
  }

  @Test
  def relation_String_Int_float_boolean(): Unit ={
    val res = graphFacade.cypher(
      """
        |CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
        |CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
        |CREATE (Keanu)-[r:ACTED_IN {roles:'CEO', age:18, money:15.5, star:false}]->(TheMatrix) return r""".stripMargin).records().next()

    val property = res("r").asInstanceOf[PandaRelationship].properties

    Assert.assertEquals("CEO", property("roles").value)
    Assert.assertEquals(18, property("age").value)
    Assert.assertEquals(15.5, property("money").value)
    Assert.assertEquals(false, property("star").value)
  }

  @Test
  def relation_List0(): Unit ={
    graphFacade.cypher(
      """
        |CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
        |CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
        |CREATE (Keanu)-[r]->(TheMatrix) return r""".stripMargin).show()
  }

  @Test
  def relation_List1(): Unit ={
    graphFacade.cypher(
      """
        |CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
        |CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
        |CREATE (Keanu)-[:ACTED_IN {roles:['Neo', 'CFO']}]->(TheMatrix)""".stripMargin).show()
  }
  @Test
  def relation_List2(): Unit ={
    graphFacade.cypher(
      """
        |CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
        |CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
        |CREATE (Keanu)-[:ACTED_IN {roles:[1, 2, 3, 4]}]->(TheMatrix)""".stripMargin).show()
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
