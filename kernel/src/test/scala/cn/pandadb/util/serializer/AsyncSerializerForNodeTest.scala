package cn.pandadb.util.serializer

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.db.{KeyValueDB, KeyValueIterator}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.Profiler.timing
import cn.pandadb.kernel.util.serializer.NodeSerializer
import cn.pandadb.util.serializer.AsyncSerializerForNodeTest.{db, getSourceIter, nodeCount}
import org.junit.{Assert, Test}

object AsyncSerializerForNodeTest {
  val dbPath: String = "/data/zzh/testDB2"
  val db: KeyValueDB = RocksDBStorage.getDB(dbPath)
  val nodeCount = 5000000
// Caution: Only enable the following code when you want to init a testdb
//  (1 to nodeCount).map(i => {
//    val properties: Map[Int, Any] = Map(1->i, 2-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      3-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      4-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      5-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      6-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      7-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")
////    val properties: Map[Int, Any] = Map(1->i)
//    val node: StoredNodeWithProperty = new StoredNodeWithProperty(i, Array(1, 2, 3), properties)
//    val bytes = NodeSerializer.serialize(node)
//    val key = NodeSerializer.serialize(i)
//    db.put(key, bytes)
//  })
//  db.flush()

  def getSourceIter: Iterator[Array[Byte]] = {
    val iter = db.newIterator()
    iter.seekToFirst()
    new Iterator[Array[Byte]]() {
      override def hasNext: Boolean = iter.isValid

      override def next(): Array[Byte] = {
        val bytes = iter.value()
        iter.next()
        bytes
      }
    }
  }

  val cores = Runtime.getRuntime.availableProcessors
  println(cores)

}
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 8:55 下午 2021/9/24
 * @Modified By:
 */
class AsyncSerializerForNodeTest {
  val sourceIter1: Iterator[Array[Byte]] = getSourceIter
  val sourceIter2: KeyValueIterator = {
    val kvIter = db.newIterator()
    kvIter.seekToFirst()
    kvIter
  }

  @Test
  def correctCheck1: Unit = {
    val iter1 = sourceIter1.map(item => NodeSerializer.deserializeNodeValue(item))
    val iter2 = NodeSerializer.parallelDeserializeNodeValue(sourceIter2)
    Assert.assertEquals(nodeCount, iter1.size)
    Assert.assertEquals(nodeCount, iter2.size)
  }

  @Test
  def correctCheck2: Unit = {
    val result1 = sourceIter1.map(item => NodeSerializer.deserializeNodeValue(item)).toArray
    val result2 = NodeSerializer.parallelDeserializeNodeValue(sourceIter2).toArray
    result1.zip(result2).foreach(pair => Assert.assertEquals(pair._1.id, pair._1.id))
  }

  @Test
  def test1(): Unit = {
    println(s"iter of the DB, as the bench:")
    timing(getSourceIter.toArray)

    println("Plain Deserialize:")
    timing(sourceIter1.map(item => NodeSerializer.deserializeNodeValue(item)).toArray)

    println(s"parallel iter, iter to Array:")
    timing(NodeSerializer.parallelDeserializeNodeValue(sourceIter2).toArray)
  }

  //The test2 is only used for developing performance, perserve it please.
  def test2(): Unit = {
    val nodeBytesArray: Array[Array[Byte]] = timing(sourceIter1.toArray)
    val iter: Iterator[Array[Byte]] = timing(nodeBytesArray.toIterator)
    val iter2: Iterator[Array[Byte]] = timing(nodeBytesArray.toIterator)
    timing(iter.map(bytes => NodeSerializer.deserializeNodeValue(bytes)).toArray)
    timing(NodeSerializer.parallelDeserializeNodeValue(iter2, 1000000))
  }

  def fakeDeserialize(bytes: Array[Byte]): StoredNodeWithProperty = {
    new StoredNodeWithProperty(1L, Array(1), Map(1->"", 2->100))
  }

}
