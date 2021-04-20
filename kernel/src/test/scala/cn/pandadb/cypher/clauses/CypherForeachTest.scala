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

class CypherForeachTest {
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
      """create (a:Person{name:'A'})
        |create (b:Person{name:'B'})
        |create (c:Person{name:'C'})
        |create (d:Person{name:'D'})
        |create (a)-[r:KNOWS]->(b)
        |create (b)-[qr:KNOWS]->(c)
        |create (c)-[qr:KNOWS]->(d)
        |""".stripMargin).show()
  }

  @Test
  def markAllNodesAlongAPath(): Unit ={
    graphFacade.cypher(
      """
        |MATCH p =(begin)-[*]->(END )
        |WHERE begin.name = 'A' AND END .name = 'D'
        |FOREACH (n IN nodes(p)| SET n.marked = TRUE )
        |""".stripMargin)
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
