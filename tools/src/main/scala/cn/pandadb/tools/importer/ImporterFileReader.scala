package cn.pandadb.tools.importer

import java.io.{BufferedInputStream, File, FileInputStream}

import scala.collection.mutable
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

//  val fileIter = this.synchronized(Source.fromFile(file).getLines())
  val fileIter: Iterator[String] = this.synchronized{
    val fis = new BufferedInputStream(new FileInputStream(file), 10 * 1024 * 1024)
    val lines = Source.fromInputStream(fis).getLines()
    lines
  }
  val supposedQueueLength: Int = Runtime.getRuntime().availableProcessors()/2
  var batchQueue: mutable.Queue[List[String]] = this.synchronized(new mutable.Queue[List[String]]())

  val fillQueue = new Runnable {
    override def run(): Unit = {
      if(batchQueue.length<supposedQueueLength && fileIter.hasNext){
        for(i<-1 to supposedQueueLength-batchQueue.length) batchQueue.enqueue(_prepareBatch)
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
      if(batchQueue.nonEmpty) batchQueue.dequeue()
      else List[String]()
    }
  }

  def notFinished: Boolean = {
    this.synchronized(fileIter.hasNext || batchQueue.nonEmpty)
  }
}
