package cn.pandadb.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.util.Profiler
import org.junit.{After, Assert, Test}

@Test
class FulltextIndexTest extends Assert {
  val dbPath = "D:\\PandaDB-tmp\\100M"
  val api = new IndexStoreAPI(dbPath)
  val nodeStore = new NodeStoreAPI(dbPath)
  val label = nodeStore.getLabelId("label0")
  val props = Array[Int](nodeStore.getPropertyKeyId("name"))
  println(label, props.toSet)

  @Test
  def create: Unit = {
    api.createIndex(label, props, true)
  }

  @Test
  def insertBatch: Unit = {
    val indexId = api.getIndexId(label, props).get
    Profiler.timing {
      api.insertFulltextIndexRecordBatch(indexId, nodeStore.getNodesByLabel(label).take(1000).map(
        n=>(props.map(n.properties.get(_).get), n.id)
      ))
    }
  }

  @Test
  def insert: Unit = {
    val indexId = api.getIndexId(label, props).get
    Profiler.timing {
      nodeStore.getNodesByLabel(label).foreach {
        n => api.insertFulltextIndexRecord(indexId, props.map(n.properties.get(_).get), n.id)
      }
    }
  }

  @Test
  def search: Unit ={
    val indexId = api.getIndexId(label, props).get
    println(api.search(indexId, props, "Bob").length)
    Profiler.timing {
      println(api.search(indexId, props, "Panda"))
    }
    Profiler.timing {
      println(api.search(indexId, props, "Panda").length)
    }
  }

  @Test
  def cleanup: Unit ={
    api.dropFulltextIndex(label, props)
    api.close()
  }
}