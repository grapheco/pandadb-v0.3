package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, NodeIndex, NodeValue, RocksDBStorage}
import org.junit.Test
import org.rocksdb
import org.rocksdb.RocksDB

import scala.util.Random

/**
 * @ClassName IndexPerformanceTest
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/15
 * @Version 0.1
 */
@Test
class IndexAndNodePerformanceTest {

  val path = "F:\\PandaDB_rocksDB\\base_1B_bak"
  val READONLE = true

  @Test
  def indexTest():Unit = {
    val db:RocksDB = if(!READONLE) RocksDB.open(path+"\\nodeIndex") else RocksDB.openReadOnly(path+"\\nodeIndex")
    // exact find
    // 10 times, each times random find 10000 records
    for (i <- 0 to 10 ){
      val keys = new Array[Int](10000).map { i =>
        val id = Random.nextInt(100000000)
        val str = id.toString.map { c => (Char.char2int(c) + 49).toChar }
        KeyHandler.nodePropertyIndexKeyToBytes(id % 10, str.getBytes(), Array(str.getBytes().length.toByte), id.toLong)
      }
      val t1 = System.currentTimeMillis()
      keys.foreach {
        key =>
          db.get(key).length
      }
      val t2 = System.currentTimeMillis()
      println(s"exact find 10000 records cost: ${t2 - t1} ms" )

    }

    val keys = new Array[Int](10).map {
      i =>
        val id = Random.nextInt(10000)
        val str = id.toString.map { c => (Char.char2int(c) + 49).toChar }
//        println(str)
        KeyHandler.nodePropertyIndexPrefixToBytes(id % 10, str.getBytes(), Array.emptyByteArray)
    }
    keys.foreach {
      key =>
        val t3 = System.currentTimeMillis()
        val iter = db.newIterator()
        iter.seek(key)
        var num = 0
        var ids:Long = 0
        while (iter.isValid && iter.key().startsWith(key)){
          num += 1
          val k = iter.key()
          ids += ByteUtils.getLong(k, k.length - 8)
//          println(ByteUtils.getLong(k, k.length - 8))
          iter.next()
        }
        val t4 = System.currentTimeMillis()
        println(s"prefix find ${num} records cost: ${t4 - t3} ms, ids = ${ids}")
    }

  }

  @Test
  def nodeTest():Unit = {
    val db:RocksDB = if(!READONLE) RocksDB.open(path+"\\nodes") else RocksDB.openReadOnly(path+"\\nodes")

    // 10 times, each times random find 10000 records
    for (i <- 0 to 1 ){
      val keys = new Array[Int](10000).map { i =>
        val id = Random.nextInt(100000000)
        KeyHandler.nodeKeyToBytes(id)
      }
      val t1 = System.currentTimeMillis()
      val values = keys.map {
        key =>
          db.get(key)
      }
      val t2 = System.currentTimeMillis()
      println(s"read 10000 nodes cost: ${t2 - t1} ms" )
      values.foreach {
        v =>
        NodeValue.parseFromBytes(v)
      }
      val t3 = System.currentTimeMillis()
      println(s"parse 10000 nodes cost: ${t3 - t2} ms" )
    }
  }

  @Test
  def nodeScanTest():Unit = {
    val db:RocksDB = if(!READONLE) RocksDB.open(path+"\\nodes") else RocksDB.openReadOnly(path+"\\nodes")
    val keyPrefix = KeyHandler.nodeKeyPrefix()
    val t0 = System.currentTimeMillis()
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    var num:Long = 0
    while (iter.isValid) {
      val node = NodeValue.parseFromBytes(iter.value())
      num += 1
      if(num % 1000000 == 0) println(s"scan ${num} nodes ${System.currentTimeMillis() - t0}" )
      iter.next()
    }
    val t1 = System.currentTimeMillis()
    println(s"scan all ${num} nodes cost: ${t1 - t0} ms" )
  }
}