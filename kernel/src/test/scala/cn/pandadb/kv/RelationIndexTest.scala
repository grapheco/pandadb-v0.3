package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.{InEdgeRelationIndexStore, OutEdgeRelationIndexStore, RocksDBStorage}
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

class RelationIndexTest {
  var inRelationIndexStore: InEdgeRelationIndexStore = null
  var outRelationIndexStore: OutEdgeRelationIndexStore = null
  var db: RocksDB = null
  @Before
  def init(): Unit ={
    if (new File("testdata/rocks/db").exists()){
      FileUtils.deleteDirectory(new File("testdata/rocks/db"))
    }
    db = RocksDBStorage.getDB()
    inRelationIndexStore = new InEdgeRelationIndexStore(db)
    outRelationIndexStore = new OutEdgeRelationIndexStore(db)
  }

  @After
  def close(): Unit ={
    db.close()
  }

  @Test
  def testIndexIn(): Unit ={
    inRelationIndexStore.setIndex(10,2,3,1, 1)
    inRelationIndexStore.setIndex(20,2,2,2, 2)
    inRelationIndexStore.setIndex(30,3,2,3, 3)
    inRelationIndexStore.setIndex(40,2,3,4, 4)
    inRelationIndexStore.setIndex(50,3,2,5, 5)
    inRelationIndexStore.setIndex(60,2,3,6, 6)
    inRelationIndexStore.setIndex(70,1,3,6, 7)
    inRelationIndexStore.setIndex(11,2,3,6, 8)

    val iter1 = inRelationIndexStore.findNodes()
    val iter2 = inRelationIndexStore.findNodes(6)
    val iter3 = inRelationIndexStore.findNodes(6, 1)
    val iter4 = inRelationIndexStore.findNodes(6, 2, 3L)

    Assert.assertEquals(8, iter1.toStream.length)
    Assert.assertEquals(3, iter2.toStream.length)
    Assert.assertEquals(1, iter3.toStream.length)
    Assert.assertEquals(2, iter4.toStream.length)

    val iter11 = inRelationIndexStore.getRelations()
    val iter21 = inRelationIndexStore.getRelations(6)
    val iter31 = inRelationIndexStore.getRelations(6, 1)
    val iter41 = inRelationIndexStore.getRelations(6, 2, 3L)

    Assert.assertEquals(8, iter11.toStream.length)
    Assert.assertEquals(3, iter21.toStream.length)
    Assert.assertEquals(1, iter31.toStream.length)
    Assert.assertEquals(2, iter41.toStream.length)

    inRelationIndexStore.deleteIndex(11,2,3,6)

    val iter66 = inRelationIndexStore.findNodes()
    val iter77 = inRelationIndexStore.getRelations()
    Assert.assertEquals(7, iter66.toStream.length)
    Assert.assertEquals(7, iter77.toStream.length)

  }

  @Test
  def testIndexOut(): Unit ={
    outRelationIndexStore.setIndex(1,2,3,11, 1)
    outRelationIndexStore.setIndex(2,2,2,21, 2)
    outRelationIndexStore.setIndex(3,3,2,31, 3)
    outRelationIndexStore.setIndex(4,2,3,41, 4)
    outRelationIndexStore.setIndex(5,3,2,51, 5)
    outRelationIndexStore.setIndex(6,2,3,61, 6)
    outRelationIndexStore.setIndex(6,1,3,62, 7)
    outRelationIndexStore.setIndex(11,2,3,63, 8)

    val iter1 = outRelationIndexStore.findNodes()
    val iter2 = outRelationIndexStore.findNodes(6)
    val iter3 = outRelationIndexStore.findNodes(6, 1)
    val iter4 = outRelationIndexStore.findNodes(6, 2, 3L)

    Assert.assertEquals(8, iter1.toStream.length)
    Assert.assertEquals(2, iter2.toStream.length)
    Assert.assertEquals(1, iter3.toStream.length)
    Assert.assertEquals(1, iter4.toStream.length)

    val iter11 = outRelationIndexStore.getRelations()
    val iter21 = outRelationIndexStore.getRelations(6)
    val iter31 = outRelationIndexStore.getRelations(6, 1)
    val iter41 = outRelationIndexStore.getRelations(6, 2, 3L)

    Assert.assertEquals(8, iter11.toStream.length)
    Assert.assertEquals(2, iter21.toStream.length)
    Assert.assertEquals(1, iter31.toStream.length)
    Assert.assertEquals(1, iter41.toStream.length)

    outRelationIndexStore.deleteIndex(11,2,3,63)

    val iter66 = outRelationIndexStore.findNodes()
    val iter77 = outRelationIndexStore.getRelations()
    Assert.assertEquals(7, iter66.toStream.length)
    Assert.assertEquals(7, iter77.toStream.length)

  }

}
