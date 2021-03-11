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

class CypherRemoveTest {
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
      """create (t:Swedish{name:'Timothy', age:25})
        |create (p:Swedish:German{name:'Peter', age:34})
        |create (a:Swedish{name:'Andy', age:36})
        |create (a)-[r:KNOWS]->(t)
        |create (a)-[qr:KNOWS]->(p)
        |""".stripMargin).show()
  }

  @Test
  def removeProperty(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (a { name: 'Andy' })
        |REMOVE a.age
        |RETURN a.name, a.age
        |""".stripMargin)
  }

  @Test
  def removeAllProperty(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (n { name: 'Peter' })
        |REMOVE n:German
        |RETURN n.name, labels(n)
        |""".stripMargin)
  }
  @Test
  def removeMutipleLabels(): Unit ={
    graphFacade.cypher(
      """
        |MATCH (n { name: 'Peter' })
        |REMOVE n:German:Swedish
        |RETURN n.name, labels(n)
        |""".stripMargin)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
