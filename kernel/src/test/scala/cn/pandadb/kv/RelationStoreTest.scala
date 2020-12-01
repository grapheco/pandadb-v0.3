package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, NoRelationGetException, RelationStore, RocksDBStorage}
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

class RelationStoreTest {
  var relationStore: RelationStore = null
  var db: RocksDB = null
  @Before
  def init(): Unit = {
    if (new File("testdata/rocks/db").exists()){
      FileUtils.deleteDirectory(new File("testdata/rocks/db"))
    }
    db = RocksDBStorage.getDB()
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
    while (iter.hasNext){
      println(iter.next().properties)
    }
  }

  @Test
  def testIndex(): Unit ={
    val keyBytes1 = KeyHandler.inEdgeKeyToBytes(1,2,3,4)
    val keyBytes2 = KeyHandler.inEdgeKeyToBytes(1,3,3,5)
    val keyBytes3 = KeyHandler.inEdgeKeyToBytes(1,4,3,6)

    val value1 = KeyHandler.relationIdToBytes(1)
    val value2 = KeyHandler.relationIdToBytes(2)
    val value3 = KeyHandler.relationIdToBytes(3)

    db.put(keyBytes1, value1)
    db.put(keyBytes2, value2)
    db.put(keyBytes3, value3)

    val iter = db.newIterator()
    val prefix = KeyHandler.relationNodeIdAndEdgeTypeIndexToBytes(1, 3)
    val array = Array[Byte](1)
    array(0) = KeyHandler.KeyType.InEdge.id.toByte
    iter.seek(array)
    val res = iter.key().slice(1, 14)

    while (iter.isValid && iter.key().slice(1, 13).sameElements(prefix)){
      val res = ByteUtils.getLong(db.get(iter.key()), 0)
      println(res)
      iter.next()
    }
  }

}
