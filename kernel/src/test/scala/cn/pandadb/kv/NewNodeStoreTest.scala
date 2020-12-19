package cn.pandadb.kv

import cn.pandadb.kernel.kv.{NewNodeStore, RocksDBStorage}
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

import java.io.File

class NewNodeStoreTest {

  var db: RocksDB = null
  val path = "D:\\PandaDB-tmp\\rocksdb\\newNodeStore"

  @Before
  def init(): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      dir.delete()
    }
    db = RocksDBStorage.getDB(path)
  }
  @After
  def clear(): Unit = {
    db.close()
  }

  @Test
  def testForSetGet(): Unit = {
    val nodeStore = new NewNodeStore(db)
    nodeStore.set(1, Array[Int](1,2,3), Map(1->1))
    val n1 = nodeStore.get(1)
    Assert.assertNotNull(n1)
    Assert.assertEquals(1, n1.id)
    Assert.assertArrayEquals(Array[Int](1,2,3), n1.labelIds)
    Assert.assertEquals(1, n1.properties.get(1).get.asInstanceOf[Int])
  }

  @Test
  def testForGetAll(): Unit = {
    db.put("a".getBytes(), "x".getBytes())

    val nodeStore = new NewNodeStore(db)
    nodeStore.set(1, Array[Int](1,2,3), Map(1->1))
    nodeStore.set(2, Array[Int](1,2,3), Map(1->1))
    nodeStore.set(3, Array[Int](1,2,3), Map(1->1))
    nodeStore.set(4, Array[Int](1,2,3), Map(1->1))

    db.put(Array(0.toByte),"x".getBytes())
    db.put(Array(5.toByte),"x".getBytes())

    Assert.assertEquals(4, nodeStore.all().toList.length)
  }

}
