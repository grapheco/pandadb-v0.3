package cn.pandadb.tools.importer

import java.io.File

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

  val fileIter = this.synchronized(Source.fromFile(file).getLines())

  val supposedQueueLength: Int = Runtime.getRuntime().availableProcessors()/2
  var batchQueue: mutable.Queue[List[String]] = new mutable.Queue[List[String]]()

  var arr: Array[String] = new Array[String](batchSize)
  var flag: Boolean = false

  val fillQueue = new Runnable {
    override def run(): Unit = {
      if(batchQueue.length<supposedQueueLength && fileIter.hasNext){
        for(i<-1 to supposedQueueLength-batchQueue.length) batchQueue.enqueue(_prepareBatch())
      }
    }
  }

  private def _prepareBatch(): List[String] = {
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

  def getLines(): List[String] = {
    this.synchronized{
      if(batchQueue.nonEmpty) batchQueue.dequeue()
      else List[String]()
    }
  }

  def notFinished: Boolean = {
    this.synchronized(fileIter.hasNext || batchQueue.nonEmpty)
  }
}
