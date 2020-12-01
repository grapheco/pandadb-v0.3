package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.{InEdgeRelationIndexStore, OutEdgeRelationIndexStore, RocksDBStorage}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}

class RelationIndexTest {
  var inRelationIndexStore: InEdgeRelationIndexStore = null
  var outRelationIndexStore: OutEdgeRelationIndexStore = null

  @Before
  def init(): Unit ={
    if (new File("testdata/rocks/db").exists()){
      FileUtils.deleteDirectory(new File("testdata/rocks/db"))
    }
    val db = RocksDBStorage.getDB()
    inRelationIndexStore = new InEdgeRelationIndexStore(db)

  }

  @Test
  def testIndex(): Unit ={
    inRelationIndexStore.setIndex(1,2,3,4, 1)
    inRelationIndexStore.setIndex(2,2,3,5, 2)
    inRelationIndexStore.setIndex(1,2,3,6, 3)
    inRelationIndexStore.setIndex(2,2,3,7, 4)
    inRelationIndexStore.setIndex(1,2,3,8, 5)
    inRelationIndexStore.setIndex(2,2,3,9, 6)

    val iter = inRelationIndexStore.getToNodesByFromNodeAndEdgeType(1, 2)
    while (iter.hasNext){
      println(iter.next())
    }
  }
}
