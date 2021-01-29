package cn.pandadb.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import cn.pandadb.kernel.util.Profiler
import org.grapheco.lynx.LynxValue
import org.junit.{Before, Test}

import scala.util.Random

/**
 * @ClassName OptionsTest
 * @Description TODO
 * @Author huchuan
 * @Date 2021/1/8
 * @Version 0.1
 */
class OptionsTest {

  var nodeStore: NodeStoreSPI = _

  @Before
  def setup(): Unit = {
    val dbPath = "F:\\PandaDB_rocksDB\\graph500"
    nodeStore = new NodeStoreAPI(dbPath)
  }

  @Test
  def seekTest(): Unit ={
    Profiler.timing({
      println("seek test:")
      var number = 0
      for (i <- 0 until 100000) {
        val id = Random.nextInt(4000000).toLong
        nodeStore.getNodeById(id).foreach(_=> number+=1)
      }
      println(s"hits: ${number}/100000")
    })
    nodeStore.close()
  }

  @Test
  def prefixTest(): Unit ={
    Profiler.timing({
      println("prefix test:")
        val res = nodeStore.getNodesByLabel(0)
        println(res.length)
    })
    nodeStore.close()
  }

  @Test
  def scanTest(): Unit ={
    Profiler.timing({
      println("scan test:")
      val res = nodeStore.allNodes()
      println(res.length)
    })
    nodeStore.close()
  }


  @Test
  def myTest(): Unit ={
    val res: List[Int] = List()
    val indexes = List()
    res.headOption
      .map(h => res.min)
      .orElse(indexes.headOption)
      .map(_+1)
      .foreach(println)
  }
}
