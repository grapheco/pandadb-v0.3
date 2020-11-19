package cn.pandadb

import java.io.File

import cn.pandadb.kernel.store.{CreateNode, LogStore, NodeSerializer, PositionMappedNodeStore, StoredNode}
import cn.pandadb.kernel.util.Profiler.timing
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}

class PerfTest {
  val COUNT: Int = 100000000

  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
  }

  @Test
  def testNodes(): Unit = {
    val nodes = new PositionMappedNodeStore(new File("./testdata/output/nodes"))
    timing {
      nodes.saveAll((1 to COUNT).iterator.map(i => StoredNode(i)))
    }

    var idx = 1
    var last: StoredNode = null
    timing {
      nodes.loadAll().foreach {
        x => {
          Assert.assertEquals(StoredNode(idx, 0, 0, 0, 0), x)
          idx += 1
        }
      }
    }

    Assert.assertEquals(COUNT + 1, idx)
  }

  @Test
  def testLogs(): Unit = {
    val logs = new LogStore(new File("./testdata/output/logs"))
    timing {
      logs._store.saveAll((1 to COUNT).iterator.map(i => CreateNode(StoredNode(i))))
    }
  }
}
