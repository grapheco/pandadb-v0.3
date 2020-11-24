package cn.pandadb.direct

import cn.pandadb.kernel.direct.{MyRelation, RelationSequenceManager, NoRelationGetException}
import org.junit.{Assert, Before, Test}

class RelationSequenceStoreTest {
  var manager: RelationSequenceManager = null
  @Before
  def init(): Unit ={
    manager = new RelationSequenceManager(100) // one directBuffer have 100 relations
    for (i <- 1 to 100){
      manager.addRelation(i, MyRelation(i, i, i))
    }
  }
  @Test(expected = classOf[IllegalArgumentException])
  def testGet(): Unit ={
    val res = manager.getRelation(50)
    Assert.assertEquals(MyRelation(50, 50, 50), res)
    manager.getRelation(0)
  }
  @Test
  def autoExpand(): Unit ={
    manager.addRelation(1000, MyRelation(11,11,11))
    Assert.assertEquals(10, manager.directBufferPageArray.length)
  }

  @Test(expected = classOf[NoRelationGetException])
  def testDelete(): Unit ={
    for (i <- 1 to 200){
      manager.addRelation(i, MyRelation(i, i, i))
    }
    manager.deleteRelation(10)
    manager.getRelation(10)
  }
}

