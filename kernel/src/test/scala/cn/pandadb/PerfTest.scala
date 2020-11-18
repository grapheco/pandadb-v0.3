package cn.pandadb

import java.io.File

import cn.pandadb.kernel.store.{PositionMappedNodeStore, StoredNode}
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
  def testLargeGraph(): Unit = {
    val nodes = new PositionMappedNodeStore(new File("./testdata/output/nodes"))
    timing {
      nodes.saveAll((1 to COUNT).iterator.map(i => StoredNode(i)))
    }

    var idx = 0
    var last: StoredNode = null
    timing {
      nodes.loadAll().foreach {
        x => {
          idx += 1
          last = x
        }
      }
    }

    Assert.assertEquals(COUNT, idx)
    Assert.assertEquals(StoredNode(COUNT, 0, 0, 0, 0), last)
  }
}
