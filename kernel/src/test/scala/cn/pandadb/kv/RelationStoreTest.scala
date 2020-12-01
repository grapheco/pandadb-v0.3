package cn.pandadb.kv

import cn.pandadb.kernel.kv.{KeyHandler, NoRelationGetException, RelationStore, RocksDBStorage}
import org.junit.{After, Assert, Before, Test}

class RelationStoreTest {
  var relationStore: RelationStore = null

  @Before
  def init(): Unit = {
    val db = RocksDBStorage.getDB()
    relationStore = new RelationStore(db)
    relationStore.setRelation(0, 1, 2, 0, 0, Map("a"->1, "b"->"c"))
    relationStore.setRelation(1, 2, 3, 0, 0, Map("a"->2, "b"->"d"))
    relationStore.setRelation(2, 3, 4, 0, 0, Map("a"->3, "b"->"e"))

  }

  @After
  def close(): Unit = {
    relationStore.close()
  }

  @Test
  def getTest(): Unit = {
    val res = relationStore.getRelation(0)
    Assert.assertEquals(Map("a"->1, "b"->"c"), res.properties)
  }

  @Test
  def isExistTest(): Unit = {
    val res1 = relationStore.relationIsExist(0)
    val res2 = relationStore.relationIsExist(1)
    val res3 = relationStore.relationIsExist(2)
    val res4 = relationStore.relationIsExist(3)

    Assert.assertEquals(true, res1)
    Assert.assertEquals(true, res2)
    Assert.assertEquals(true, res3)

    Assert.assertEquals(false, res4)
  }

  @Test
  def updateValue(): Unit = {
    relationStore.setRelation(0, 1, 2, 0,0, Map[String, Any]("ccc"->222.asInstanceOf[Object]))

    val res1 = relationStore.getRelation(0)
    Assert.assertEquals(Map("ccc"->222), res1.properties)
  }

  @Test(expected = classOf[NoRelationGetException])
  def delete(): Unit = {
    relationStore.deleteRelation(0)
    relationStore.getRelation(0)
  }

  @Test
  def iterator(): Unit = {
    val iter = relationStore.getAll()
    Assert.assertEquals(3, iter.toStream.length)
  }
}
