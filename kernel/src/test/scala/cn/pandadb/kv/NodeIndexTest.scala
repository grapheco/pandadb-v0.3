package cn.pandadb.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.{ByteUtils, NodeIndex, RocksDBStorage}
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.{ReadOptions, RocksDB}
import org.apache.commons.lang3.RandomStringUtils

import scala.tools.nsc.profile.Profiler
import scala.util.Random

@Test
class NodeIndexTest extends Assert {


  val path = "C:\\rocksDB"
  val hddPath = "D:\\rocksDB"

  @Test
  def rocksDBTest = {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test1")
    Assert.assertNotNull(db)

    db.put("aaa".getBytes(),"bbb".getBytes())
    db.put("hello".getBytes(),"world".getBytes())
    db.put("hello1".getBytes(),"world1".getBytes())
    db.put("hello2".getBytes(),"world2".getBytes())
    db.put("eee".getBytes(),"fff".getBytes())
    db.put("zzz".getBytes(),"zzzz".getBytes())
    db.put("hello3".getBytes(),"world3".getBytes())
    db.put("hhh".getBytes(),"hhh".getBytes())

    Assert.assertArrayEquals(db.get("hello".getBytes()), "world".getBytes())
    Assert.assertNull(db.get("hel".getBytes()))

    val iter = db.newIterator()
    val prefix = "hel".getBytes()
    iter.seek(prefix)
    while(iter.isValid) {
      println(iter.value().length)
      if (iter.value().length>0) {
        println(new String(iter.value()))
      }
      //      Assert.assertArrayEquals(iter.value(), "world".getBytes())
      iter.next()
    }
    iter.close()
    db.close()
  }

  def long2Bytes(long: Long)={
    val b = new Array[Byte](8)
    ByteUtils.setLong(b,0,long)
    b
  }

  @Test
  def indexBaseTest= {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test2")
    val LABEL = 1
    val PROPS = Array[Int](1)
    val ni = new NodeIndex(db)

    // drop index
    ni.dropIndex(LABEL, PROPS)

    // create index
    val indexId = ni.createIndex(LABEL, PROPS)
    val indexId2 = ni.createIndex(LABEL, PROPS)
    assert(indexId == indexId2)

    // get Index id
    val indexId3 = ni.getIndexId(LABEL, PROPS)
    assert(indexId == indexId3)

    // insert index
    val data = (0 until 50).iterator.map(_.toLong).map{
      l=>
        val v = long2Bytes(l/10)
        (v, Array[Byte](v.length.toByte), l)
    }
    ni.insertIndex(ni.getIndexId(LABEL, PROPS),data)

    // find 20-29
    Assert.assertArrayEquals(ni.find(indexId, long2Bytes(2)).toArray,
      Array[Long](20,21,22,23,24,25,26,27,28,29)
    )

    // create and index2
    val LABEL2 = 2
    val PROPS2 = Array[Int](2)
    val indexId4 = ni.createIndex(LABEL2, PROPS2)
    assert(indexId4 == ni.getIndexId(LABEL2, PROPS2))
    val data2 = (0 until 50).iterator.map(_.toLong).map{
      l=>
        val v = long2Bytes(l%10)
        (v, Array[Byte](v.length.toByte), l)
    }
    ni.insertIndex(ni.getIndexId(LABEL2, PROPS2), data2)

    //drop index1
    ni.dropIndex(LABEL, PROPS)

    // find 20-29 by index 1
    assert(ni.find(indexId, long2Bytes(2)).toArray.length==0)

    // find 1,11,21,31,41 by index 2
    Assert.assertArrayEquals(ni.find(ni.getIndexId(LABEL2, PROPS2), long2Bytes(1)).toArray,
      Array[Long](1,11,21,31,41))
  }

  @Test
  def indexBigTest= {
//    val db:RocksDB = RocksDBStorage.getDB(path+"/test3")
//    val ni = new NodeIndex(db)
//    val t0 = System.currentTimeMillis()
//    for (node <- 0 until 10000000) {
//      ni.writeIndexRow(1003, long2Bytes(scala.util.Random.nextInt(1000).toLong), node.toLong)
//    }
//    val t1 = System.currentTimeMillis()
//    println("create time: ", t1-t0)//47624
//    for (i <- 0 until 1000){
//      ni.find(1003, long2Bytes(100.toLong)).toList.length
//    }
//    val t2 = System.currentTimeMillis()
//    println("search 1000 time: ", t2-t1)//2774
  }

  @Test
  def stringIndexTest = {
    val data = Map[Int,String](
      0->"张三",
      1->"李四",
      2->"王五",
      3->"张三",
      4->"张三丰",
      5->"李四光",
      6->"王五",
      7->"Scala",
      8->"PandaDB",
      9->"PandaDB is a Intelligent Graph Database.").map{
      v=> val value = v._2.getBytes()
        (value, Array[Byte](value.length.toByte), v._1.toLong)
    }.iterator
    val db:RocksDB = RocksDBStorage.getDB(path+"/test5")
    val ni = new NodeIndex(db)
    // create and insert
    val indexId = ni.createIndex(5,Array[Int](5))
    ni.insertIndex(indexId, data)
    // search
    Assert.assertArrayEquals(Array[Long](0, 3), ni.find(indexId, "张三".getBytes()).toArray)
    Assert.assertArrayEquals(Array[Long](4), ni.find(indexId, "张三丰".getBytes()).toArray)
    Assert.assertArrayEquals(Array[Long](8), ni.find(indexId, "PandaDB".getBytes()).toArray)
    Assert.assertArrayEquals(Array[Long](9), ni.find(indexId, "PandaDB is a Intelligent Graph Database.".getBytes()).toArray)

  }

}