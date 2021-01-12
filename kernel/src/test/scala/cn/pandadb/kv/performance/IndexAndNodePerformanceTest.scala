package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.index.IndexEncoder
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.util.Profiler
import cn.pandadb.kernel.util.serializer.NodeSerializer
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

  val path = "F:\\PandaDB_rocksDB\\graph500"
  val READONLE = false

  @Test
  def indexTest():Unit = {
    val db: RocksDB = if (!READONLE) RocksDB.open(path + "\\nodeIndex") else RocksDB.openReadOnly(path + "\\nodeIndex")
    // exact find
    val epoch = 10
    var time: Long = 0
    for (i <- 1 to epoch) {
      val t0 = System.currentTimeMillis()
      val keys = new Array[Int](10000).map { i =>
        val id = Random.nextInt(100000000)
        val str = id.toString.map { c => (Char.char2int(c) + 49).toChar }
        KeyConverter.toIndexKey(id % 10, IndexEncoder.STRING_CODE, IndexEncoder.encode(str), id.toLong)
      }
      val t1 = System.currentTimeMillis()
      println(s"create 10000 keys cost: ${t1 - t0} ms")
      keys.foreach {
        key =>
          db.get(key).length
      }
      val t2 = System.currentTimeMillis()
      println(s"exact find 10000 records cost: ${t2 - t1} ms")
      time += t2 - t1
    }
    println(s"avg: ${time / epoch} ms")
  }

  @Test
  def indexRangeTest():Unit = {
    val db: RocksDB = if (!READONLE) RocksDB.open(path + "\\nodeIndex") else RocksDB.openReadOnly(path + "\\nodeIndex")
    val epoche2 = 10
    val keys = new Array[Int](epoche2).map {
      i =>
        val id = Random.nextInt(10)
        val str = id.toString.map { c => (Char.char2int(c) + 49).toChar }
        println(str)
        KeyConverter.toIndexKey(id % 10, IndexEncoder.STRING_CODE, IndexEncoder.encode(str), id.toLong)
    }
    var time2:Long = 0
    keys.foreach {
      key =>
        val t3 = System.currentTimeMillis()
        val iter = db.newIterator()
        iter.seek(key)
        var num = 0
        var ids:Long = 0
        while (iter.isValid && iter.key().startsWith(key) && iter.key().length-key.length>=9){
          num += 1
          val k = iter.key()
          ids += ByteUtils.getLong(k, k.length - 8) % 10
//          println(ByteUtils.getLong(k, k.length - 8))
          iter.next()
        }
        val t4 = System.currentTimeMillis()
        println(s"prefix find ${num} records cost: ${t4 - t3} ms, ids = ${ids}")
        time2 += t4 -t3
    }
    println(s"avg: ${time2/epoche2} ms")
  }

  @Test
  def nodeTest():Unit = {
    val db:RocksDB = if(!READONLE) RocksDB.open(path+"\\nodes") else RocksDB.openReadOnly(path+"\\nodes")

    // 10 times, each times random find 10000 records
    val epoch = 10
    var allRead:Long = 0
    var allParse:Long = 0
    for (i <- 1 to epoch ){
      val keys = new Array[Int](10000).map { i =>
        val id = Random.nextInt(1000000)
//        println(id)
        KeyConverter.toNodeKey(id%10, id.toLong)
      }
      val t1 = System.currentTimeMillis()
      val values = keys.map {
        key =>
          db.get(key)
      }
      val t2 = System.currentTimeMillis()
      println(s"read 10000 nodes cost: ${t2 - t1} ms" )
      allRead += t2 - t1
      values.foreach {
        v =>
        NodeSerializer.deserializeNodeValue(v)
      }
      val t3 = System.currentTimeMillis()
      println(s"parse 10000 nodes cost: ${t3 - t2} ms" )
      allParse += t3 - t2
    }
    println(s"avg read: ${allRead/epoch} ms, avg parse: ${allParse/epoch} ms" )
  }

  @Test
  def nodeScanTest():Unit = {
    val db:RocksDB = if(!READONLE) RocksDB.open(path+"\\nodes") else RocksDB.openReadOnly(path+"\\nodes")
    val keyPrefix = KeyConverter.toNodeKey(0)
    val t0 = System.currentTimeMillis()
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    var num:Long = 0
    while (iter.isValid) {
      val node = NodeSerializer.deserializeNodeValue(iter.value())
      num += 1
      if(num % 1000000 == 0) println(s"scan ${num} nodes ${System.currentTimeMillis() - t0}" )
      iter.next()
    }
    val t1 = System.currentTimeMillis()
    println(s"scan all ${num} nodes cost: ${t1 - t0} ms" )
  }

  @Test
  def nodeAPIPerformanceTest():Unit={
    val nodeStore = new NodeStoreAPI(path)
    Profiler.timing{
      var count = 0
      for (i <- 1 to 10000) {
        val id = Random.nextInt(2000000)
        val n = nodeStore.getNodeById(id)
        if(n!=null) n.get.properties
        else count +=1
      }
      println(count)
    }
  }
}
