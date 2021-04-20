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

class CypherUnionTest {
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
      """create (a:Actor{name:'Anthony Hopkins'})
        |create (b:Actor{name:'Hitchcock'})
        |create (c:Actor{name:'Helen Mirren'})
        |create (m:Movie{title:'Hitchcock'})
        |create (a)-[:KNOWS]->(c)
        |create (a)-[:ACTS_IN]->(m)
        |create (c)-[:ACTS_IN]->(m)
        |""".stripMargin).show()
  }

  @Test
  def combine2QueriesAndRetainDuplicates(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION ALL MATCH (n:Movie)
        |RETURN n.title AS name
        |""".stripMargin)
  }
  @Test
  def combine2QueriesAndRemoveDuplicates(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (n:Actor)
        |RETURN n.name AS name
        |UNION
        |MATCH (n:Movie)
        |RETURN n.title AS name
        |""".stripMargin)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
