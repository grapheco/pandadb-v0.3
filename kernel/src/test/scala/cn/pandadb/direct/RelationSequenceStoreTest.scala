package cn.pandadb.direct

import cn.pandadb.kernel.direct.{NoRelationGetException, RelationSequenceManager}
import cn.pandadb.kernel.store.StoredRelation
import org.junit.{Assert, Before, Test}
class RelationSequenceStoreTest {
  var manager: RelationSequenceManager = null

  @Before
  def init(): Unit ={
    manager = new RelationSequenceManager(100) // one directBuffer have 100 relations
    for (i <- 1 to 100){
      manager.addRelation(i, i, i, i)
    }
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testGet(): Unit ={
    val res = manager.getRelation(50)
    Assert.assertEquals(StoredRelation(50, 50, 50, 50), res)
    manager.getRelation(0)
  }
  @Test
  def autoExpand(): Unit ={
    manager.addRelation(1000, 11,11,11)
    Assert.assertEquals(10, manager.directBufferArray.length)
  }

  @Test(expected = classOf[NoRelationGetException])
  def testDelete(): Unit ={
    for (i <- 100 to 200){
      manager.addRelation(i, i, i, i)
    }
    manager.deleteRelation(10)
    manager.getRelation(10)
  }

  @Test
  def testIterator(): Unit ={
    manager.addRelation(2333, 23,33,33)
    manager.addRelation(5555, 44,33,22)
    val iter = manager.getAllRelations()
    manager.deleteRelation(100)
    Assert.assertEquals(101, iter.toStream.size)
  }
}
