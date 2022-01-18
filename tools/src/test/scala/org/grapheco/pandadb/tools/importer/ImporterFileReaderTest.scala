package org.grapheco.pandadb.tools.importer

import java.io.File
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingDeque, ScheduledExecutorService, TimeUnit}

import org.junit.Test

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:27 2020/12/30
 * @Modified By:
 */
class ImporterFileReaderTest {
  @Test
  def test3(): Unit = {
    val file = new File("./src/test/output/headTest.csv")
    val reader = new ImporterFileReader(file, ",", 100)
    (1 to 10).foreach(i =>println(reader.getHead.getAsString))
  }
}
