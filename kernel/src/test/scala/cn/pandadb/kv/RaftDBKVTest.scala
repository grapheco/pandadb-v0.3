package cn.pandadb.kv

import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.kv.db.raftdb.RaftDB
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.Profiler
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.junit.{After, Assert, Before, Test}

import java.io.File
import scala.util.Random

@Test
class RaftDBKVTest {

//  val sourceNodesDB = new NodeStoreAPI("D:\\PandaDB-tmp\\100M")

  val db = new RaftDB(Configs.DB_PATH, Configs.RAFT_DATA_PATH, Configs.ALL_NODE_ADDRESSES, Configs.CLUSTER_NAME)

  @Test
  def put(): Unit ={
    Profiler.timing(
      (1 to 2).foreach(i => {db.aPut(Array(i.toByte), Array(Random.nextInt().toByte))})
    )
    val iters = 100
    var cnt = 0
    val totalCNT = 9998
    Profiler.timing {
      (1 to iters).foreach { j=> {
          val fs = (1 to totalCNT).map(i => db.aPut(Array(i.toByte), Array(Random.nextInt().toByte)))
          fs.foreach(f => f.join())
        }
      }
    }
    println("complete")
  }

  @Test
  def get(): Unit ={
    Profiler.timing(
      (1 to 2).foreach(i => {db.aGet(Array(i.toByte))})
    )
    val iters = 100
    var cnt = 0
    val totalCNT = 9998
    Profiler.timing {
      (1 to iters).foreach { j=> {
        val fs = (1 to totalCNT).map(i => db.aGet(Array(i.toByte)))
        fs.foreach(f => f.join())
      }
      }
    }
    println("complete")
  }

  @Test
  def iterate(): Unit ={
    Profiler.timing(
      (1 to 2).foreach(i => {db.aGet(Array(i.toByte))})
    )
    val iters = 100
    var cnt = 0
    val totalCNT = 9998
    val iter = db.newIterator()
    iter.seekToFirst()


    val itr = new Iterator[Array[Byte]] (){
      override def hasNext: Boolean = iter.isValid
      override def next(): Array[Byte] = {
        val v = iter.value()
        iter.next()
        v
      }
    }

    Profiler.timing {
      for (elem <- itr.take(1000)) {println(elem)}
    }
    println("complete")
  }

  @After
  def end(): Unit = {
    db.close()
  }

}
