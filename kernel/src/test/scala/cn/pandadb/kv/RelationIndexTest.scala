package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.relation.{RelationInEdgeIndexStore, RelationOutEdgeIndexStore}
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

class RelationIndexTest {
  var relationInEdgeIndex: RelationInEdgeIndexStore = null
  var relationOutEdgeIndex: RelationOutEdgeIndexStore = null
  var db: RocksDB = null
  @Before
  def init(): Unit ={
    if (new File("testdata/rocks/db").exists()){
      FileUtils.deleteDirectory(new File("testdata/rocks/db"))
    }
    db = RocksDBStorage.getDB()
    relationInEdgeIndex = new RelationInEdgeIndexStore(db)
    relationOutEdgeIndex = new RelationOutEdgeIndexStore(db)
  }

  @After
  def close(): Unit ={
    db.close()
  }

  @Test
  def testInEdge(): Unit ={

    relationInEdgeIndex.setIndex(10,1,0,1, 1)
    relationInEdgeIndex.setIndex(11,2,1,1, 2)
    relationInEdgeIndex.setIndex(12,3,0,1, 3)
    relationInEdgeIndex.setIndex(13,0,0,1, 4)
    relationInEdgeIndex.setIndex(14,0,0,1, 5)
    relationInEdgeIndex.setIndex(15,2,0,1, 6)
    relationInEdgeIndex.setIndex(16,2,1,1, 7)
    relationInEdgeIndex.setIndex(17,2,0,2, 8)

    val iter1 = relationInEdgeIndex.findNodes()
    Assert.assertEquals(8, iter1.toStream.length)

    val iter2 = relationInEdgeIndex.findNodes(1)
    Assert.assertEquals(7, iter2.toStream.length)

    val iter3 = relationInEdgeIndex.findNodes(1, 2)
    Assert.assertEquals(3, iter3.toStream.length)
    val iter31 = relationInEdgeIndex.findNodes(1, 0)
    Assert.assertEquals(2, iter31.toStream.length)
    val iter32 = relationInEdgeIndex.findNodes(1, 1)
    Assert.assertEquals(1, iter32.toStream.length)

    val iter4 = relationInEdgeIndex.findNodes(1, 2, 1L)
    Assert.assertEquals(2, iter4.toStream.length)
    val iter41 = relationInEdgeIndex.findNodes(1, 0, 0L)
    Assert.assertEquals(2, iter41.toStream.length)

    relationInEdgeIndex.deleteIndex(17,2,0,2)
    val iter11 = relationInEdgeIndex.findNodes()
    Assert.assertEquals(7, iter11.toStream.length)
  }

  @Test
  def testIndexOut(): Unit ={

    relationOutEdgeIndex.setIndex(1,1,0,10, 1)
    relationOutEdgeIndex.setIndex(1,2,1,11, 2)
    relationOutEdgeIndex.setIndex(1,3,0,12, 3)
    relationOutEdgeIndex.setIndex(1,0,0,13, 4)
    relationOutEdgeIndex.setIndex(1,0,0,14, 5)
    relationOutEdgeIndex.setIndex(1,2,0,15, 6)
    relationOutEdgeIndex.setIndex(1,2,1,16, 7)
    relationOutEdgeIndex.setIndex(2,2,0,17, 8)

    val iter1 = relationOutEdgeIndex.findNodes()
    Assert.assertEquals(8, iter1.toStream.length)

    val iter2 = relationOutEdgeIndex.findNodes(1)
    Assert.assertEquals(7, iter2.toStream.length)

    val iter3 = relationOutEdgeIndex.findNodes(1, 2)
    Assert.assertEquals(3, iter3.toStream.length)
    val iter31 = relationOutEdgeIndex.findNodes(1, 0)
    Assert.assertEquals(2, iter31.toStream.length)
    val iter32 = relationOutEdgeIndex.findNodes(1, 1)
    Assert.assertEquals(1, iter32.toStream.length)

    val iter4 = relationOutEdgeIndex.findNodes(1, 2, 1L)
    Assert.assertEquals(2, iter4.toStream.length)
    val iter41 = relationOutEdgeIndex.findNodes(1, 0, 0L)
    Assert.assertEquals(2, iter41.toStream.length)

    relationOutEdgeIndex.deleteIndex(2,2,0,17)
    val iter11 = relationOutEdgeIndex.findNodes()
    Assert.assertEquals(7, iter11.toStream.length)
  }

}
