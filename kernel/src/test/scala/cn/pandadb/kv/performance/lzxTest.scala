package cn.pandadb.kv.performance

import cn.pandadb.kernel.kv.node.NodeStore
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBGraphAPI, RocksDBStorage}
import org.junit.Test
import org.rocksdb.ReadOptions

import scala.collection.mutable
import scala.util.Random

class RocksDBGraphAPITest {

  private val nodeDB = RocksDBStorage.getDB(s"testdata/nodes")
  private val nodeStore = new NodeStore(nodeDB)

  @Test
  def testNodeStore(): Unit = {
    /*
    write 1000000 nodes(id, null, null), time(ms): 12982
    write 1000000 nodes(id, Array(1,2,3), null), time(ms): 12882
    write 1000000 nodes(id, Array(1,2,3), Map('t1'->'t1', 't2'->2, 't3'->true)), time(ms): 15182
    read all nodes (count 1000000), time(ms): 19328
    read all nodes (count 1000000) without deserializer, time(ms): 1598
     */
    /*
    write 1000000 nodes(id, null, null), time(ms): 10622
write 1000000 nodes(id, Array(1,2,3), null), time(ms): 10971
write 1000000 nodes(id, Array(1,2,3), Map('t1'->'t1', 't2'->2, 't3'->true)), time(ms): 11634
read all nodes (count 1000000), time(ms): 2624
read all nodes (count 1000000) without deserialized, time(ms): 814
Map(t1 -> t1, t2 -> 2, t3 -> true)
      */
    val t1 = System.currentTimeMillis()
    val nodesCount = 10000*100

    for (i <- 1 to nodesCount) {
      nodeStore.set(i, null, null)
    }
    val t2 = System.currentTimeMillis()

    for (i <- 1 to nodesCount) {
      nodeStore.set(i, Array(1,2,3), null)
    }
    val t3 = System.currentTimeMillis()

    for (i <- 1 to nodesCount) {
      nodeStore.set(i, Array(1,2,3), Map("t1"->"t1", "t2"->2, "t3"->true))
    }
    val t4 = System.currentTimeMillis()

    val nodes = nodeStore.all()
    val readNodeCount = nodes.size
    val t5 = System.currentTimeMillis()

    val keyPrefix = KeyHandler.nodeKeyPrefix()
    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = nodeDB.newIterator(readOptions)
    iter.seek(keyPrefix)

    val nodeValueIter = new Iterator[Array[Byte]] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(keyPrefix)

      override def next(): Array[Byte] = {
        //        val node = NodeValue.parseFromBytes(iter.value())
        val node = iter.value()
        iter.next()
        node
      }
    }
    val readNodeValueCount = nodeValueIter.size
    val t6 = System.currentTimeMillis()

    println(s"write ${nodesCount} nodes(id, null, null), time(ms): ${(t2-t1)}")
    println(s"write ${nodesCount} nodes(id, Array(1,2,3), null), time(ms): ${(t3-t2)}")
    println(s"write ${nodesCount} nodes(id, Array(1,2,3), Map('t1'->'t1', 't2'->2, 't3'->true)), time(ms): ${(t4-t3)}")
    println(s"read all nodes (count ${readNodeCount}), time(ms): ${(t5-t4)}")
    println(s"read all nodes (count ${readNodeValueCount}) without deserialized, time(ms): ${(t6-t5)}")

    val nodes2 = nodeStore.all()
    println(nodes2.next().properties)
  }


  @Test
  def testRocksDB(): Unit = {
    //write 1000000 KV, time(ms): 8360
    //read all 1000000 KV, time(ms): 159
    val t1 = System.currentTimeMillis()
    val nodesCount = 10000*100L
    for (i <- 1L to nodesCount) {
      nodeDB.put(ByteUtils.longToBytes(i), Array[Byte]())
    }
    val t2 = System.currentTimeMillis()

    val iter = nodeDB.newIterator()
    iter.seekToFirst()
    val recordIterartor = new Iterator[Array[Byte]] (){
      override def hasNext: Boolean = iter.isValid()

      override def next(): Array[Byte] = {
        val node = iter.key()
        iter.next()
        node
      }
    }
    val recordCount = recordIterartor.size
    val t3 = System.currentTimeMillis()

    println(s"write ${nodesCount} KV, time(ms): ${(t2-t1)}")
    println(s"read all ${nodesCount} KV, time(ms): ${(t3-t2)}")
  }


  @Test
  def testSerialize(): Unit = {
    import org.nustaq.serialization.FSTConfiguration
    val conf: FSTConfiguration = FSTConfiguration.createDefaultConfiguration

    val r = new Random()
    val t1 = System.currentTimeMillis()
    for (i <- 1 to 10000*1000) {
      val m1 = new mutable.HashMap[Any, Any]()
      m1(1) = i
      m1(2) = r.nextLong().toString
      m1("bbb") =  Map(1->1,"2"->2)
//      val bytes2: Array[Byte] = conf.asByteArray(Map(1->i, 2->i, "B"-> Map(1->1,"2"->2)))
      val bytes2: Array[Byte] = conf.asByteArray(m1)
//      println(bytes2.size)
//      conf.asObject(bytes2)
    }
    val t2 = System.currentTimeMillis()
    println(t2-t1)


//    println(bytes2.length)
//    println(conf.asObject(bytes2))

  }


}
