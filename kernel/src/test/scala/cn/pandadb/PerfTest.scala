package cn.pandadb

import java.io.File

import cn.pandadb.kernel.store.{NodeStore, StoredNode}
import cn.pandadb.kernel.util.Profiler.timing
import org.apache.commons.io.FileUtils
import org.junit.{Before, Test}

class PerfTest {
  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
  }

  @Test
  def testWriteLargeGraph(): Unit = {
    val nodes = new NodeStore(new File("./testdata/output/nodes"))
    timing {
      nodes.saveAll((1 to 100000000).toStream.map(StoredNode(_)))
    }
  }

  @Test
  def testPrintLargeGraph(): Unit = {
    timing {
      (1 to 100000000).toStream.map(StoredNode(_)).foreach(println(_))
    }
  }
}
