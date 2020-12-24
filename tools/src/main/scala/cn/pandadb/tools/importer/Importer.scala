package cn.pandadb.tools.importer

import java.io.{File, FileInputStream}
import java.util.concurrent.ScheduledExecutorService

import cn.pandadb.kernel.PDBMetaData

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:09 2020/12/18
 * @Modified By:
 */
trait Importer {

  protected var propSortArr: Array[Int]
  protected val headMap: Map[Int, String]
  val importerFileReader: ImporterFileReader

  protected val coreNum: Int = Runtime.getRuntime().availableProcessors()

  val service: ScheduledExecutorService
  val closer = new Runnable {
    override def run(): Unit = {
      if(!importerFileReader.notFinished) service.shutdown()
    }
  }

  def importData(): Unit = {
    var taskId: Int = -1
    val taskArray: Array[Future[Boolean]] = new Array[Int](coreNum/2).map(item => Future{taskId += 1; _importTask(taskId)})
    taskArray.foreach(task => Await.result(task, Duration.Inf))
  }
  protected def _importTask(taskId: Int): Boolean

  protected def _setHead(propStartIndex: Int, headFile: File): Map[Int, String] = {
    var hMap: Map[Int, String] = Map[Int, String]()
    val headArr = Source.fromFile(headFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[Int](headArr.length - propStartIndex)
    // headArr(0) is :ID, headArr(1) is :LABELS
    for (i <- propStartIndex until headArr.length) {
      val fieldArr = headArr(i).split(":")
      val propId: Int = PDBMetaData.getPropId(fieldArr(0))
      propSortArr(i - propStartIndex) = propId
      val propType: String = {
        if(fieldArr.length == 1) "string"
        else fieldArr(1).toLowerCase()
      }
      hMap += (propId -> propType)
    }
    hMap
  }
  def estLineCount(file: File): Long = {
    val fileSize: Long = file.length() // count by Byte
    if(fileSize < 1024*1024) {
      Source.fromFile(file).getLines().size
    } else {
      // get 1/1000 of the file to estimate line count.
      val fis: FileInputStream = new FileInputStream(file)
      val sampleSize: Int = (fileSize/1000).toInt
      val bytes: Array[Byte] = new Array[Byte](sampleSize)
      fis.read(bytes)
      val sampleCount = new String(bytes, "utf-8").split("\n").length
      val lineCount = fileSize/sampleSize * sampleCount
      lineCount
    }
  }
}