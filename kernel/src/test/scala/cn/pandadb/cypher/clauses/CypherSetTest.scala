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

// https://neo4j.com/docs/cypher-manual/3.5/clauses/set/

class CypherSetTest {
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
      """create (n{name:'Stefan'})
        |create (m{name:'George'})
        |create (nn{name:'Peter', age:34})
        |create (q:Swedish{name:'Andy', age:36, hungry:true})
        |create (n)-[r:KNOWS]->(q)
        |create (q)-[qr:KNOWS]->(nn)
        |create (m)-[rr:KNOWS]->(nn)
        |""".stripMargin).show()
  }

  @Test
  def setAProperty(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'Andy' })
                         |SET n.surname = 'Taylor'
                         |RETURN n.name, n.surname""".stripMargin).records().next()
    println(res)
  }

  @Test
  def setAProperty2(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'Andy' })
                                   |SET (
                                   |CASE
                                   |WHEN n.age = 36
                                   |THEN n END ).worksIn = 'Malmo'
                                   |RETURN n.name, n.worksIn""".stripMargin).records().next()
    println(res)
  }
  @Test
  def setAProperty3(): Unit ={
    //will return null
    val res = graphFacade.cypher("""MATCH (n { name: 'Andy' })
                                   |SET (
                                   |CASE
                                   |WHEN n.age = 55
                                   |THEN n END ).worksIn = 'Malmo'
                                   |RETURN n.name, n.worksIn""".stripMargin).records().next()
    println(res)
  }
  @Test
  def updateAProperty(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'Andy' })
                                   |SET n.age = toString(n.age)
                                   |RETURN n.name, n.age""".stripMargin).records().next()
    println(res)
  }
  @Test
  def removeAProperty(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'Andy' })
                                   |SET n.name = NULL RETURN n.name, n.age""".stripMargin).records().next()
    println(res)
  }
  @Test
  def copyProperties(): Unit ={
    val res = graphFacade.cypher("""MATCH (at { name: 'Andy' }),(pn { name: 'Peter' })
                                   |SET at = pn
                                   |RETURN at.name, at.age, at.hungry, pn.name, pn.age""".stripMargin).records().next()
    println(res)
  }
  @Test
  def replacePropertiesUsingMap(): Unit ={
    val res = graphFacade.cypher("""MATCH (p { name: 'Peter' })
                                   |SET p = { name: 'Peter Smith', position: 'Entrepreneur' }
                                   |RETURN p.name, p.age, p.position""".stripMargin).records().next()
    println(res)
  }
  @Test
  def removeAllPropertiesUsingEmptyMap(): Unit ={
    val res = graphFacade.cypher("""MATCH (p { name: 'Peter' })
                                   |SET p = { }
                                   |RETURN p.name, p.age""".stripMargin).records().next()
    println(res)
  }
  @Test
  def mutateSpecificPropertiesUsingAMap(): Unit ={
    val res = graphFacade.cypher("""MATCH (p { name: 'Peter' })
                                   |SET p += { age: 38, hungry: TRUE , position: 'Entrepreneur' }
                                   |RETURN p.name, p.age, p.hungry, p.position""".stripMargin).records().next()
    println(res)
  }
  @Test
  def mutateSpecificPropertiesUsingAMap2(): Unit ={
    val res = graphFacade.cypher("""MATCH (p { name: 'Peter' })
                                   |SET p += { }
                                   |RETURN p.name, p.age""".stripMargin).records().next()
    println(res)
  }
  @Test
  def setMultiplePropertiesUsingOneSetClause(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'Andy' })
                                   |SET n.position = 'Developer', n.surname = 'Taylor' """.stripMargin).records().next()
    println(res)
  }
  @Test
  def setLabelOnNode(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'Stefan' })
                                   |SET n:German
                                   |RETURN n.name, labels(n) AS labels""".stripMargin).records().next()
    println(res)
  }
  @Test
  def setMutipleLabelsOnNode(): Unit ={
    val res = graphFacade.cypher("""MATCH (n { name: 'George' })
                                   |SET n:Swedish:Bossman
                                   |RETURN n.name, labels(n) AS labels""".stripMargin).records().next()
    println(res)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
