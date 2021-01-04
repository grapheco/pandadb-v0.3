package cn.pandadb.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import org.junit.{After, Assert, Test}

@Test
class FulltextIndexTest extends Assert {
  val api = new IndexStoreAPI("D:\\PandaDB-tmp\\100M")
  val label = 1
  val props = Array[Int](2)

  @Test
  def create: Unit = {
    api.createFulltextIndex(label, props)
  }

  @After
  def cleanup: Unit ={
    api.dropFulltextIndex(label, props)
    api.close()
  }
}