package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.junit.{After, Before, Test}

class CypherMergeTest {
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
    createSense()
  }
  def createSense(): Unit ={
    graphFacade.cypher(
      """create (a:Person{chauffeurName:'John Brown', name:'Charlie Sheen', bornIn:'New York'})
        |create (b:Person{chauffeurName:'Bil White', name:'Oliver Stone', bornIn:'New York'})
        |create (c:Person{chauffeurName:'John Brown', name:'Michael Douglas', bornIn:'New Jersey'})
        |create (d:Person{chauffeurName:'Bob Brown', name:'Martin Sheen', bornIn:'Ohio'})
        |create (e:Person{chauffeurName:'Ted Green', name:'Rob Reiner', bornIn:'New York'})
        |create (m1:Movie{title:'Wall Street'})
        |create (m2:Movie{title:'The American President'})
        |create (a)-[:FATHER]->(d)
        |create (b)-[:ACTED_IN]->(m1)
        |create (c)-[:ACTED_IN]->(m1)
        |create (c)-[:ACTED_IN]->(m2)
        |create (d)-[:ACTED_IN]->(m1)
        |create (d)-[:ACTED_IN]->(m2)
        |create (e)-[:ACTED_IN]->(m2)
        |""".stripMargin).show()
  }

  @Test
  def  mergeSingleNodeWithLabel(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (robert:Critic)
        |RETURN robert, labels(robert)
        |""".stripMargin)
  }

  @Test
  def  mergeSingleNodeWithProperty(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (charlie { name: 'Charlie Sheen', age: 10 })
        |RETURN charlie
        |""".stripMargin)
  }
  @Test
  def  mergeSingleNodeWithLabelAndProperty(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (michael:Person { name: 'Michael Douglas' })
        |RETURN michael.name, michael.bornIn
        |""".stripMargin)
  }
  @Test
  def mergeSingleNodeDerivedFromExistingNodeProperty(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (person:Person)
        |MERGE (city:City { name: person.bornIn })
        |RETURN person.name, person.bornIn, city
        |""".stripMargin)
  }
  @Test
  def mergeWithOnCreate(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (keanu:Person { name: 'Keanu Reeves' })
        |ON CREATE SET keanu.created = timestamp()
        |RETURN keanu.name, keanu.created
        |""".stripMargin)
  }
  @Test
  def mergeWithOnMatch(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (person:Person)
        |ON MATCH SET person.found = TRUE RETURN person.name, person.found
        |""".stripMargin)
  }
  @Test
  def mergeWithOnCreateOnMatch(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (keanu:Person { name: 'Keanu Reeves' })
        |ON CREATE SET keanu.created = timestamp()
        |ON MATCH SET keanu.lastSeen = timestamp()
        |RETURN keanu.name, keanu.created, keanu.lastSeen
        |""".stripMargin)
  }
  @Test
  def mergeWithOnMatchSettingMultipleProperties(): Unit ={
    graphFacade.cypher(
      """
        |MERGE (person:Person)
        |ON MATCH SET person.found = TRUE , person.lastAccessed = timestamp()
        |RETURN person.name, person.found, person.lastAccessed
        |""".stripMargin)
  }
  @Test
  def mergeOnRelationship(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (charlie:Person { name: 'Charlie Sheen' }),(wallStreet:Movie { title: 'Wall Street' })
        |MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
        |RETURN charlie.name, type(r), wallStreet.title
        |""".stripMargin)
  }
  @Test
  def mergeOnMultipleRelationship(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (oliver:Person { name: 'Oliver Stone' }),(reiner:Person { name: 'Rob Reiner' })
        |MERGE (oliver)-[:DIRECTED]->(movie:Movie)<-[:ACTED_IN]-(reiner)
        |RETURN movie
        |""".stripMargin)
  }
  @Test
  def mergeOnUndirectedRelationship(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (charlie:Person { name: 'Charlie Sheen' }),(oliver:Person { name: 'Oliver Stone' })
        |MERGE (charlie)-[r:KNOWS]-(oliver)
        |RETURN r
        |""".stripMargin)
  }
  @Test
  def mergeOnRelationshipBetween2existingNodes(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (person:Person)
        |MERGE (city:City { name: person.bornIn })
        |MERGE (person)-[r:BORN_IN]->(city)
        |RETURN person.name, person.bornIn, city
        |""".stripMargin)
  }
  @Test
  def mergeOnRelationshipBetweenExistingNodeAndMergedNodeDerivedFromNodeProperty(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (person:Person)
        |MERGE (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur { name: person.chauffeurName })
        |RETURN person.name, person.chauffeurName, chauffeur
        |""".stripMargin)
  }
  @Test
  def constrainWithMergeIfNodeNotExistCreate(): Unit ={
    graphFacade.cypher(
      """
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE;
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.role IS UNIQUE;
        |MERGE (laurence:Person { name: 'Laurence Fishburne' })
        |RETURN laurence.name
        |""".stripMargin)
  }
  @Test
  def constrainWithMergeMatchExistingNode(): Unit ={
    graphFacade.cypher(
      """
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE;
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.role IS UNIQUE;
        |MERGE (oliver:Person { name: 'Oliver Stone' })
        |RETURN oliver.name, oliver.bornIn
        |""".stripMargin)
  }
  @Test
  def constrainWithMergePartialMatches(): Unit ={
    //will fail
    graphFacade.cypher(
      """
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE;
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.role IS UNIQUE;
        |MERGE (michael:Person { name: 'Michael Douglas', role: 'Gordon Gekko' })
        |RETURN michael
        |""".stripMargin)
    graphFacade.cypher(
      """
        |MERGE (michael:Person { name: 'Michael Douglas' })
        |SET michael.role = 'Gordon Gekko'
        |""".stripMargin)
  }
  @Test
  def constrainWithMergeConflictingMatches(): Unit ={
    // will failed
    graphFacade.cypher(
      """
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE;
        |CREATE CONSTRAINT ON (n:Person) ASSERT n.role IS UNIQUE;
        |MERGE (oliver:Person { name: 'Oliver Stone', role: 'Gordon Gekko' })
        |RETURN oliver
        |""".stripMargin)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
