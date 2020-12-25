//package cn.pandadb
//
//import java.io.File
//
//import cn.pandadb.kernel.store.{CreateNode, LogStore, NodeSerializer, NodeStoreImpl, StoredNode}
//import cn.pandadb.kernel.util.Profiler.timing
//import org.apache.commons.io.FileUtils
//import org.junit.{Assert, Before, Test}
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration.Duration
//import scala.concurrent.{Await, Future}
//
//class PerfTest {
//  val COUNT: Int = 100000000
//
//  @Before
//  def setup(): Unit = {
//    FileUtils.deleteDirectory(new File("./testdata/output"))
//    new File("./testdata/output").mkdirs()
//  }
//
//  @Test
//  def testNodes(): Unit = {
//    val nodes = new NodeStoreImpl(new File("./testdata/output/nodes"), new File("./testdata/output/nodesmap"))
//    timing {
//      nodes.saveAll((1 to COUNT).iterator.map(i => StoredNode(i)))
//    }
//
//    var idx = 1
//    var last: StoredNode = null
//    timing {
//      nodes.loadAll().foreach {
//        x => {
//          Assert.assertEquals(StoredNode(idx), x)
//          idx += 1
//        }
//      }
//    }
//
//    Assert.assertEquals(COUNT + 1, idx)
//  }
//
//  @Test
//  def testLogs(): Unit = {
//    val logs = new LogStore(new File("./testdata/output/logs"))
//    timing {
//      logs._store.saveAll((1 to COUNT).iterator.map(i => CreateNode(StoredNode(i))), _ => {})
//    }
//  }
//
//  @Test
//  def testMultiLogs(): Unit = {
//    val futures = (1 to 5).map(i => Future {
//      val logs = new LogStore(new File(s"./testdata/output/logs.$i"))
//      logs._store.saveAll((i * COUNT to i * COUNT + COUNT - 1).iterator.map(x => CreateNode(StoredNode(x))), _ => {})
//    })
//
//    timing {
//      futures.map(Await.result(_, Duration.Inf))
//    }
//  }
//
//}
