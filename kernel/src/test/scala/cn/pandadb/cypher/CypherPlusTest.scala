package cn.pandadb.cypher

import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.junit.{After, Before, Test}

class CypherPlusTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _

  @Before
  def init(): Unit ={
    val dbPath = "./testbase"

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
  def create(): Unit ={
    graphFacade.cypher(
      s"""
        Create(n:Person{name:'zzh',photo:Blob.fromURL('https://wx4.sinaimg.cn/mw690/005OnbIely1gosftobq8pj304h05ht9e.jpg')});
        """.stripMargin).show()
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }
}
