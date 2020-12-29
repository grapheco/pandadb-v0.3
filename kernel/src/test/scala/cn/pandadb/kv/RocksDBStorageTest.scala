package cn.pandadb.kv

import cn.pandadb.kernel.kv.RocksDBStorage
import org.junit.{Assert, Before, Test}

class RocksDBStorageTest {

  @Test
  def testGetDB(): Unit = {
    val db = RocksDBStorage.getDB("testdata/db")
    Assert.assertNotNull(db)
  }


}
