package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}

import scala.collection.mutable.ArrayBuffer

class MatchTest {

  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @Before
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
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
  }

  @After
  def closeDB():Unit = {
    db.close()
  }


  @Test
  def matchTest(): Unit = {
    val cyphers = ArrayBuffer[(String, String, Any)]()  //(cypherName, cypher, expectedSize)
    // basic node finding
    cyphers.append(("MatchAllNodes", "MATCH (n) RETURN n", 7))
    cyphers.append(("MatchAllNodesWithLabel", "MATCH (movie:Movie) RETURN movie.title", 2))
    cyphers.append(("MatchAllNodesWithMultiLabels", "MATCH (n:Director:Person) RETURN n", 2))
    cyphers.append(("MatchAllNodesWithMultiLabels", "MATCH (n:Director:Person) RETURN n", 2))
    cyphers.append(("MatchNodesSpecifyingProperty", "MATCH (n{name:'Oliver Stone'}) RETURN n", 1))
    cyphers.append(("MatchNodesSpecifyingProperties", "MATCH (n{name:'Oliver Stone', sex:'male'}) RETURN n", 1))
    cyphers.append(("MatchNodesWithLabelSpecifyingProperties", "MATCH (n:Person{name:'Oliver Stone', sex:'male'}) RETURN n", 1))

    cyphers.append(("MatchRelatedNodes", "MATCH (director {name: 'Oliver Stone'})--(movie) RETURN movie.title", 1))
    cyphers.append(("MatchWithLabels", "MATCH (:Person {name: 'Oliver Stone'})--(movie:Movie) RETURN movie.title", 1))

    // relationship basics
    cyphers.append(("OutgoingRelationships", "MATCH (:Person {name: 'Oliver Stone'})-->(movie) RETURN movie.title", 1))
    cyphers.append(("OutgoingRelationships", "MATCH (:Person {name: 'Oliver Stone'})-->(movie) RETURN movie.title", 1))
    cyphers.append(("DirectedRelationshipsAndVariable", "MATCH (:Person {name: 'Oliver Stone'})-[r]->(movie) RETURN type(r)", 1))
    cyphers.append(("MatchOnRelationshipType", "MATCH (wallstreet:Movie {title: 'Wall Street'})<-[:ACTED_IN]-(actor) RETURN actor.name", 3))
    cyphers.append(("MatchOnMultipleRelationshipTypes", "MATCH (wallstreet {title: 'Wall Street'})<-[:ACTED_IN|:DIRECTED]-(person) RETURN person.name", 4))
    cyphers.append(("MatchOnRelationshipRypeAndUseAVariable", "MATCH (wallstreet {title: 'Wall Street'})<-[r:ACTED_IN]-(actor) RETURN r.role", 3))

    // relationships in depth
    cyphers.append(("CreateRelationshipTypesWithUncommonCharacters",
      "MATCH  (charlie:Person {name: 'Charlie Sheen'}), (rob:Person {name: 'Rob Reiner'})CREATE (rob)-[:`TYPE INCLUDING A SPACE`]->(charlie)", 0))
    cyphers.append(("MatchRelationshipTypesWithUncommonCharacters", "MATCH (n {name: 'Rob Reiner'})-[r:`TYPE INCLUDING A SPACE`]->() RETURN type(r)", 1))
    cyphers.append(("MultipleRelationships", "MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN]->(movie)<-[:DIRECTED]-(director) RETURN movie.title, director.name", 1))
    cyphers.append(("VariableLengthRelationships", "MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN*1..3]-(movie:Movie)\nRETURN movie.title", 3))
    cyphers.append(("Variable length relationships with multiple relationship types", "MATCH (charlie {name: 'Charlie Sheen'})-[:ACTED_IN|DIRECTED*2]-(person:Person)\nRETURN person.name", 3))
    cyphers.append(("Relationship variable in variable length relationships", "MATCH p = (actor {name: 'Charlie Sheen'})-[:ACTED_IN*2]-(co_actor)\nRETURN relationships(p)", 2))
    cyphers.append(("Match with properties on a variable length path: Create",
      """MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (martin:Person {name: 'Martin Sheen'})
        |CREATE (charlie)-[:X {blocked: false}]->(:UNBLOCKED)<-[:X {blocked: false}]-(martin)
        |CREATE (charlie)-[:X {blocked: true}]->(:BLOCKED)<-[:X {blocked: false}]-(martin)""".stripMargin.replaceAll("\r\n", " "), 0))
    cyphers.append(("Match with properties on a variable length path: match",
      """MATCH p = (charlie:Person)-[* {blocked:false}]-(martin:Person)
        |WHERE charlie.name = 'Charlie Sheen' AND martin.name = 'Martin Sheen'
        |RETURN p""".stripMargin.replaceAll("\r\n", " "), 1))
    cyphers.append(("Zero length paths", "MATCH (wallstreet:Movie {title: 'Wall Street'})-[*0..1]-(x) RETURN x", 5))
    cyphers.append(("Named paths", "MATCH p = (michael {name: 'Michael Douglas'})-->() RETURN p", 2))
    cyphers.append(("Matching on a bound relationship", "MATCH (a)-[r]-(b) WHERE id(r) = 0 RETURN a, b", 2))

    // shortest path
    cyphers.append(("Single shortest path",
      """MATCH
        |  (martin:Person {name: 'Martin Sheen'}),
        |  (oliver:Person {name: 'Oliver Stone'}),
        |  p = shortestPath((martin)-[*..15]-(oliver))
        |RETURN p""".stripMargin.replaceAll("\r\n", " "), 1))
    cyphers.append(("Single shortest path with predicates",
      """MATCH
        |  (charlie:Person {name: 'Charlie Sheen'}),
        |  (martin:Person {name: 'Martin Sheen'}),
        |  p = shortestPath((charlie)-[*]-(martin))
        |WHERE none(r IN relationships(p) WHERE type(r) = 'FATHER')
        |RETURN p""".stripMargin.replaceAll("\r\n", " "), 1))
    cyphers.append(("All shortest paths",
      """MATCH
        |  (martin:Person {name: 'Martin Sheen'} ),
        |  (michael:Person {name: 'Michael Douglas'}),
        |  p = allShortestPaths((martin)-[*]-(michael))
        |RETURN p""".stripMargin.replaceAll("\r\n", " "), 2))

    // Get node or relationship by id
    cyphers.append(("Node by id", "MATCH (n) WHERE id(n) = 1 RETURN n", 1))
    cyphers.append(("Relationship by id", "MATCH ()-[r]->() WHERE id(r) = 1 RETURN r", 1))
    cyphers.append(("Multiple nodes by id", "MATCH (n) WHERE id(n) IN [1, 3, 5] RETURN n", 3))

    var pass = true
    cyphers.foreach(item => {
      try{
        Assert.assertEquals(item._3, db.cypher(item._2).records().size)
        println("pass", item._2)
      }catch {
        case e: Any => {
          println("fail", item._2)
          e.printStackTrace()
          pass = false
        }
      }
    })

    Assert.assertTrue("not all passed", pass)
  }

  @Test
  def testForArray(): Unit = {
    db.cypher("create(n:person:worker{name:'xx',arr1:[],arr2:[1,2,3],arr3:[1.1,3.5], arr4:['abc','dd','g'],arr5:[true]}) return n").show()
    val res = db.cypher("match(n:person{name:'xx'}) return n").records()
    Assert.assertEquals(1, res.size)
  }


}
