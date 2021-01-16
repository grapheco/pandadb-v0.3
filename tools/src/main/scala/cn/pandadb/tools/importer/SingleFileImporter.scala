package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData

import java.io.{File, FileInputStream}
import java.util.concurrent.{Executors, ScheduledExecutorService}
import org.apache.logging.log4j.scala.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 9:35 2021/1/15
 * @Modified By:
 */
trait SingleFileImporter extends Logging{
  val csvFile: File
  val idIndex: Int
  val labelIndex: Int
  val importerFileReader: ImporterFileReader
  val headLine: Array[String]
  val estLineCount: Long
  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val taskCount: Int

  // [index, (propId, propTypeId)]
  lazy val propHeadMap: Map[Int, (Int, String)] = {
//    val buffer = headLine.toBuffer
//    buffer -= headLine(idIndex)
//    buffer -= headLine(labelIndex)
//    buffer.remove(idIndex)
//    buffer.remove(labelIndex)
    headLine.zipWithIndex.map(item => {
      if(item._2 == idIndex || item._2 == labelIndex){
        (-1, (-1, ""))
      } else {
        val pair = item._1.split(":")
        val propId = PDBMetaData.getPropId(pair(0))
        val propType = {
          if (pair.length == 2) pair(1).toLowerCase()
          else "string"
        }
        (item._2, (propId, propType))
      }
    }).toMap.filter(item => item._1 > -1)
  }

  val closer = new Runnable {
    override def run(): Unit = {
      if(!importerFileReader.notFinished) {
        service.shutdown()
      }
    }
  }

  protected def _importTask(taskId: Int): Boolean

  protected def _getPropMap(lineArr: Array[String], propHeadMap: Map[Int, (Int, String)]): Map[Int, Any] = {
    var propMap: Map[Int, Any] = Map[Int, Any]()
    propHeadMap.foreach(kv => {
      val index = kv._1
      val propId = kv._2._1
      val propValue: Any = {
        kv._2._2 match {
          case "long" => lineArr(index).toLong
          case "int" => lineArr(index).toInt
          case "boolean" => lineArr(index).toBoolean
          case "double" => lineArr(index).toBoolean
          case _ => lineArr(index).replace("\"", "")
        }
      }
      propMap += (propId -> propValue)
    })
    propMap
  }

  def estLineCount(file: File): Long = {
    CSVIOTools.estLineCount(file)
  }

  def importData(): Unit = {
    val taskId: AtomicInteger = new AtomicInteger(0)
    val taskArray: Array[Future[Boolean]] = new Array[Int](taskCount).map(item => Future{_importTask(taskId.getAndIncrement())})
    taskArray.foreach(task => {Await.result(task, Duration.Inf)})
  }

}
