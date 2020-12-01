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
  def testIn(): Unit ={
    inRelationIndexStore.addSrcIdAndEdgeType(1,0,2)
    inRelationIndexStore.addSrcIdAndEdgeType(1,0,3)
    inRelationIndexStore.addSrcIdAndEdgeType(1,0,4)

    inRelationIndexStore.addSrcIdAndCategory(2,0,5)
    inRelationIndexStore.addSrcIdAndCategory(2,0,6)
    inRelationIndexStore.addSrcIdAndCategory(2,0,7)

    Assert.assertEquals(Set(2,3,4), inRelationIndexStore.getToNodesBySrcIdAndEdgeType(1, 0))
    Assert.assertEquals(Set(5,6,7), inRelationIndexStore.getToNodesBySrcIdAndCategory(2, 0))

    inRelationIndexStore.deleteIndexOfSrcIdAndEdgeType2ToNode(1, 0, 2)
    inRelationIndexStore.deleteIndexOfSrcIdAndCategory2ToNode(2, 0, 5)

    Assert.assertEquals(Set(3,4), inRelationIndexStore.getToNodesBySrcIdAndEdgeType(1, 0))
    Assert.assertEquals(Set(6,7), inRelationIndexStore.getToNodesBySrcIdAndCategory(2, 0))
  }

  @Test
  def testOut(): Unit ={
    outRelationIndexStore.addDestIdAndEdgeType(1,0,2)
    outRelationIndexStore.addDestIdAndEdgeType(1,0,3)
    outRelationIndexStore.addDestIdAndEdgeType(1,0,4)

    outRelationIndexStore.addDestAndCategory(2,0,5)
    outRelationIndexStore.addDestAndCategory(2,0,6)
    outRelationIndexStore.addDestAndCategory(2,0,7)

    Assert.assertEquals(Set(2,3,4), outRelationIndexStore.getFromNodesByDestIdAndEdgeType(1, 0))
    Assert.assertEquals(Set(5,6,7), outRelationIndexStore.getFromNodesByDestIdAndCategory(2, 0))

    outRelationIndexStore.deleteIndexOfDestIdAndEdgeType2ToNode(1, 0, 2)
    outRelationIndexStore.deleteIndexOfDestIdAndCategory2ToNode(2, 0, 5)

    Assert.assertEquals(Set(3,4), outRelationIndexStore.getFromNodesByDestIdAndEdgeType(1, 0))
    Assert.assertEquals(Set(6,7), outRelationIndexStore.getFromNodesByDestIdAndCategory(2, 0))
  }
}
