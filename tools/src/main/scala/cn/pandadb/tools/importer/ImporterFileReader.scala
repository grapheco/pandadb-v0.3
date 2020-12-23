package cn.pandadb.tools.importer

import java.io.File
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 16:39 2020/12/22
 * @Modified By:
 */
class ImporterFileReader(file: File ,batchSize: Int = 1000000) {

  val fileIter = this.synchronized(Source.fromFile(file).getLines())

  private def _prepareBatch(): Array[String] = {
    this.synchronized{
      var innercount = 0
      val arrayBuf: ArrayBuffer[String] = new ArrayBuffer[String]()
      while (fileIter.hasNext && innercount < batchSize) {
        arrayBuf.append(fileIter.next())
        innercount += 1
      }
      arrayBuf.toArray
    }
  }

  def getLines(): Array[String] = {
    this.synchronized{
      _prepareBatch()
    }
  }

  def notFinished: Boolean = {
    this.synchronized(fileIter.hasNext)
  }

}
