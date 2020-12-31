package cn.pandadb.tools.importer

import java.io.File
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 16:39 2020/12/22
 * @Modified By:
 */

class ImporterFileReader(file: File ,batchSize: Int = 1000000) {

//TODO: implement a thread-safe file reader

  val fileIter = this.synchronized(Source.fromFile(file).getLines())

  val supposedQueueLength: Int = Runtime.getRuntime().availableProcessors()/2
  var batchQueue: BlockingQueue[List[String]] = new LinkedBlockingQueue[List[String]](supposedQueueLength)

  val fillQueue = new Runnable {
    override def run(): Unit = {
      if(fileIter.hasNext) {
        batchQueue.put(_prepareBatch)
      }
    }
  }

  private def _prepareBatch: List[String] = {
    this.synchronized{
      var innercount = 0
      val listBuf: ListBuffer[String] = new ListBuffer[String]()
      while (fileIter.hasNext && innercount < batchSize) {
        listBuf.append(fileIter.next())
        innercount += 1
      }
      listBuf.toList
    }
  }

  def getLines: List[String] = {
    this.synchronized{
      if(batchQueue.isEmpty) List[String]()
      else batchQueue.take()
    }
  }

  def notFinished: Boolean = {
    this.synchronized(fileIter.hasNext || batchQueue.size()>0)
  }
}
