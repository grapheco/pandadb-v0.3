package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.{KeyHandler, RocksDBStorage, TokenStore}
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

class TokenStoreTest {
  var labelStore1: LabelStoreTest = _

  @Before
  def init(): Unit = {
    labelStore1 = new LabelStoreTest
  }

  @After
  def close(): Unit = {
    labelStore1.close()
  }

  @Test
  def testSet(): Unit = {
    labelStore1.set("name")
    labelStore1.set("age")
    labelStore1.set("name")
    Assert.assertEquals(2, labelStore1.mapInt2String.keySet.size)
    Assert.assertEquals(2, labelStore1.mapString2Int.keySet.size)
    Assert.assertEquals(2, labelStore1.idGenerator.get())
  }

  @Test
  def testDelete(): Unit = {
    labelStore1.set("name")
    labelStore1.set("age")
    labelStore1.set("country")
    labelStore1.set("artist")

    labelStore1.delete("age")

    labelStore1.set("heihei")

    Assert.assertEquals(4, labelStore1.mapInt2String.keySet.size)
    Assert.assertEquals(4, labelStore1.mapString2Int.keySet.size)
    Assert.assertEquals(5, labelStore1.idGenerator.get())
  }

  @Test
  def testSaveAllAndLoadAll(): Unit ={
    var store = new LabelStoreTest2
    store.set("name")
    store.set("age")
    store.set("country")
    store.set("artist")
    store.set("music")
    store.set("singer")
    store.delete("artist")
    store.saveAll()
    store.close()

    store = new LabelStoreTest2
    store.loadAll()

    Assert.assertEquals(5, store.mapInt2String.keySet.size)
    Assert.assertEquals(5, store.mapString2Int.keySet.size)
    Assert.assertEquals(6, store.idGenerator.get())
    store.close()

    FileUtils.deleteDirectory(new File("testdata/rocks/dbb"))
  }

}

class LabelStoreTest extends TokenStore {
  override val db: RocksDB = {
    if (new File("testdata/rocks/db").exists()) {
      FileUtils.deleteDirectory(new File("testdata/rocks/db"))
    }
    RocksDBStorage.getDB()
  }
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.labelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.labelKeyPrefixToBytes
}

class LabelStoreTest2 extends TokenStore {
  override val db: RocksDB = {
    RocksDBStorage.getDB("testdata/rocks/dbb")
  }
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.labelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.labelKeyPrefixToBytes
}