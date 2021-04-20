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

class CypherUniqueTest {
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
      """create (r{name:'root'})
        |create (a{name:'A'})
        |create (b{name:'B'})
        |create (c:{name:'C'})
        |create (r)-[:X]->(a)
        |create (r)-[:X]->(c)
        |create (r)-[:X]->(b)
        |""".stripMargin).show()
  }

  @Test
  def createUniqueNodes(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (root { name: 'root' })
        |CREATE UNIQUE (root)-[:LOVES]-(someone)
        |RETURN someone
        |""".stripMargin)
  }
  @Test
  def createNodesWithValues(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (root { name: 'root' })
        |CREATE UNIQUE (root)-[:X]-(leaf { name: 'D' })
        |RETURN leaf
        |""".stripMargin)
  }
  @Test
  def createLabeledNodeIfMissing(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (a { name: 'A' })
        |CREATE UNIQUE (a)-[:KNOWS]-(c:blue)
        |RETURN c
        |""".stripMargin)
  }
  @Test
  def createRelationshipIfMissing(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (lft { name: 'A' }),(rgt)
        |WHERE rgt.name IN ['B', 'C']
        |CREATE UNIQUE (lft)-[r:KNOWS]->(rgt)
        |RETURN r
        |""".stripMargin)
  }
  @Test
  def createRelationshipWithValues(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (root { name: 'root' })
        |CREATE UNIQUE (root)-[r:X { since: 'forever' }]-()
        |RETURN r
        |""".stripMargin)
  }
  @Test
  def describeComplexPattern(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (root { name: 'root' })
        |CREATE UNIQUE (root)-[:FOO]->(x),(root)-[:BAR]->(x)
        |RETURN x
        |""".stripMargin)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
