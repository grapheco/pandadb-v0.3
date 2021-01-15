package cn.pandadb.tools.importer

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
//  val file = new File("D://GitSpace//ScalaUtils//output//edges1kw.csv")
//  val fileReader = new ImporterFileReader(file, ",", 500000)
//
//  val closer = new Runnable {
//    override def run(): Unit = {
//      if(!fileReader.notFinished) {
//        service.shutdown()
//      }
//    }
//  }
//
////  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
//  val service: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
//  service.scheduleWithFixedDelay(fileReader.fillQueue, 0, 50, TimeUnit.MILLISECONDS)
//  service.scheduleAtFixedRate(closer, 1, 1, TimeUnit.SECONDS)
//
//
//
//  @Test
//  def test(): Unit = {
//    var count = 0
//    while (fileReader.notFinished) {
//      val lines = fileReader.getLines
//      lines.foreach(i => count += 1)
//      if(count % 1000000 == 0) println(count)
//      Thread.sleep(10)
//    }
//    println(count)
//  }
//
//  @Test
//  def test2() = {
//    val queue: BlockingQueue[List[String]] = new LinkedBlockingDeque[List[String]](10)
//    println(queue.isEmpty)
//  }

  @Test
  def test3(): Unit = {
    val file = new File("./src/test/output/headTest.csv")
    val reader = new ImporterFileReader(file, ",", 100)
    (1 to 10).foreach(i =>println(reader.getHead.getAsString))
  }


}
