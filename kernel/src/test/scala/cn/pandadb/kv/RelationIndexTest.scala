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
    outRelationIndexStore = new OutEdgeRelationIndexStore(db)
  }

  @Test
  def testIndex(): Unit ={
    inRelationIndexStore.setIndex(0,2,3,1, 1)
    inRelationIndexStore.setIndex(0,2,2,2, 2)
    inRelationIndexStore.setIndex(0,3,2,3, 3)
    inRelationIndexStore.setIndex(0,2,3,4, 4)
    inRelationIndexStore.setIndex(0,3,2,5, 5)
    inRelationIndexStore.setIndex(0,2,3,6, 6)

    val iter1 = inRelationIndexStore.getAllToNodes(0, 3)
    val iter2 = inRelationIndexStore.getAllToNodes(0, 3L)

    Assert.assertEquals(2, iter1.toStream.length)
    Assert.assertEquals(3, iter2.toStream.length)
  }

  @Test
  def testIndex2(): Unit ={
    outRelationIndexStore.setIndex(1,2,3,0, 1)
    outRelationIndexStore.setIndex(2,2,2,0, 2)
    outRelationIndexStore.setIndex(3,3,2,0, 3)
    outRelationIndexStore.setIndex(4,2,3,0, 4)
    outRelationIndexStore.setIndex(5,3,2,0, 5)
    outRelationIndexStore.setIndex(6,2,3,0, 6)

    val iter1 = outRelationIndexStore.getAllFromNodes(0, 3)
    val iter2 = outRelationIndexStore.getAllFromNodes(0, 3L)

    Assert.assertEquals(2, iter1.toStream.length)
    Assert.assertEquals(3, iter2.toStream.length)
  }

}
