package cn.pandadb

import java.io.File

import cn.pandadb.pnode.store.{FileBasedNodeStore, Node}
import cn.pandadb.pnode.util.Profiler.timing
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
    val nodes = new FileBasedNodeStore(new File("./testdata/output/nodes"))
    timing {
      nodes.save((1 to 100000000).toStream.map(Node(_)))
    }
  }

  @Test
  def testPrintLargeGraph(): Unit = {
    timing {
      (1 to 100000000).toStream.map(Node(_)).foreach(println(_))
    }
  }
}
