package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}

class CypherDeleteTest {

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
      """create (n:Person{name:'Andy', age:36})
        |create (m:Person{name:'Timothy', age:25})
        |create (q:Person{name:'Peter', age:34})
        |create (w:Person{name:'UNKNOWN'})
        |create (n)-[r:KNOWS]->(m)
        |create (n)-[qr:KNOWS]->(q)
        |""".stripMargin).show()
  }

  @Test
  def deleteSingleNode(): Unit ={
    val size = graphFacade.cypher("match (n:Person{name:'UNKNOWN'}) delete n").records().size
    Assert.assertEquals(0, size)
  }

  @Test
  def deleteAllNodesAndRelationship(): Unit ={
    val size = graphFacade.cypher("match (n) detach delete n").records().size
    Assert.assertEquals(0, size)
  }
  @Test
  def deleteRelationshipOnly(): Unit ={
    val size = graphFacade.cypher("MATCH (n { name: 'Andy' })-[r:KNOWS]->() DELETE r").records().size
    Assert.assertEquals(0, size)
  }


  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
