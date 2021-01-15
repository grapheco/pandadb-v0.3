package cn.pandadb.tools.importer

import java.io.{File, FileInputStream}
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 9:35 2021/1/15
 * @Modified By:
 */
trait SingleFileImporter {
//  val csvIter: Iterator[CSVLine]
  val headLine: Array[String]
  val idIndex: Int
  val labelIndex: Int
  val importerFileReader: ImporterFileReader

  // [index, (propId, propTypeId)]
  val propHeadMap: Map[Int, (Int, String)]

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
