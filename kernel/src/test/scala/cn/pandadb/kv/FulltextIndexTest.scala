package cn.pandadb.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.util.Profiler
import org.junit.{After, Assert, Test}

@Test
class FulltextIndexTest extends Assert {
  val dbPath = "D:\\PandaDB-tmp\\100M.2"
  val api = new IndexStoreAPI(dbPath)
  val nodeStore = new NodeStoreAPI(dbPath)
  val label = 1
  val props = Array[Int](3)

  @Test
  def create: Unit = {
    api.createFulltextIndex(label, props)
  }

  @Test
  def insert: Unit = {
    val indexId = api.createFulltextIndex(label, props)
    nodeStore.allNodes().take(100).foreach {
      n =>
        api.insertFulltextIndexRecord(indexId, props.map(n.properties.get(_).get), n.id)
    }
    Profiler.timing {
      nodeStore.allNodes().take(1000).foreach {
        n =>
          api.insertFulltextIndexRecord(indexId, props.map(n.properties.get(_).get), n.id)
      }
    }
    indexId
  }

  @Test
  def search: Unit ={
    insert
    val indexId = api.createFulltextIndex(label, props)
    println(api.search(indexId, props, "Bob").length)
    Profiler.timing {
      api.search(indexId, props, "Panda").length
    }
  }

  @After
  def cleanup: Unit ={
    api.dropFulltextIndex(label, props)
    api.close()
  }
}