package cn.pandadb.util.serializer

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.db.{KeyValueDB, KeyValueIterator}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.Profiler.timing
import cn.pandadb.kernel.util.serializer.RelationSerializer
import cn.pandadb.util.serializer.AsyncSerializerForRelTest.{db, getSourceIter, relCount}
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 2:29 下午 2021/9/27
 * @Modified By:
 */
object AsyncSerializerForRelTest {
  val dbPath: String = "/data/zzh/testDB_500wRel"
  val db: KeyValueDB = RocksDBStorage.getDB(dbPath)
  val relCount = 5000000

  // Caution: Only enable the following code when you want to init a testdb
//  (1 to relCount).map(i => {
//    val properties: Map[Int, Any] = Map(1->i, 2-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      3-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      4-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      5-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      6-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
//      7-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")
//    val relWithProps: StoredRelationWithProperty = new StoredRelationWithProperty(i.toLong, from = i+1, to = i + 2, typeId = i%10 ,properties)
//    val bytes = RelationSerializer.serialize(relWithProps)
//    val key = RelationSerializer.serialize(i.toLong)
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

class AsyncSerializerForRelTest {
  val sourceIter1: Iterator[Array[Byte]] = getSourceIter
  val sourceIter2: KeyValueIterator = {
    val kvIter = db.newIterator()
    kvIter.seekToFirst()
    kvIter
  }

  @Test
  def correctCheck1: Unit = {
    val iter1 = sourceIter1.map(item => RelationSerializer.deserializeRelWithProps(item))
    val iter2 = RelationSerializer.deserializeRelWithProps(sourceIter2)
    Assert.assertEquals(relCount, iter1.size)
    Assert.assertEquals(relCount, iter2.size)
  }

  @Test
  def correctCheck2: Unit = {
    val result1 = sourceIter1.map(item => RelationSerializer.deserializeRelWithProps(item)).toArray
    val result2 = RelationSerializer.deserializeRelWithProps(sourceIter2).toArray
    result1.zip(result2).foreach(pair => Assert.assertEquals(pair._1.id, pair._1.id))
  }

  @Test
  def test0(): Unit = {
    println(s"iter of the DB, as the bench")
    timing(getSourceIter.toArray)
  }

  @Test
  def test1(): Unit = {
    println(s"Plain Deserialize $relCount relationships")
    timing(sourceIter1.map(item => RelationSerializer.deserializeRelWithProps(item)).toArray)
  }

  @Test
  def test2(): Unit = {
    println(s"parallel iter, iter to Array")
    timing(RelationSerializer.deserializeRelWithProps(sourceIter2).toArray)
  }
}