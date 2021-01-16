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
  val cmd: ImportCmd

  // [index, (propId, propTypeId)]
  val propHeadMap: Map[Int, (Int, String)]

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
          case "string" => lineArr(index).replace("\"", "")
          case "date" => lineArr(index).replace("\"", "")
          case "long[]" => lineArr(index).split(cmd.arrayDelimeter).map(item => item.toLong)
          case "int[]" => lineArr(index).split(cmd.arrayDelimeter).map(item => item.toInt)
          case "string[]" => lineArr(index).split(cmd.arrayDelimeter).map(item => item.replace("{","").replace("}",""))
          case "boolean[]" => lineArr(index).split(cmd.arrayDelimeter).map(item => item.toBoolean)
          case "double[]" => lineArr(index).split(cmd.arrayDelimeter).map(item => item.toDouble)
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
