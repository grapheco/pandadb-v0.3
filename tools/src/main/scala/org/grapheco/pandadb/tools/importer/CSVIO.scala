package org.grapheco.pandadb.tools.importer

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 16:46 2021/1/11
 * @Modified By:
 */
class CSVReader(file: File, spliter: String) {
  val source = Source.fromFile(file)
  val iter = source.getLines()

  def getAsCSVLines: Iterator[CSVLine] = source.getLines().map(line => new CSVLine(line.split(spliter, -1)))
  def close: Unit = source.close()
}

class CSVWriter(target: File) {
  val bos = new BufferedOutputStream(new FileOutputStream(target))

  def close: Unit = {
    bos.flush()
    bos.close()
  }

  def write(bytes: Array[Byte]): Unit = {
    bos.write(bytes)
    bos.flush()
  }
  def write(lineArr: Array[String]): Unit = write(s"${lineArr.mkString("|")}\n".getBytes())
  def write(line: String): Unit = write(s"$line\n".getBytes)
}

class CSVLine(arr: Array[String]) {
  private val _lineArrayBuffer: ArrayBuffer[String] = new ArrayBuffer[String]() ++ arr

  def insertElemAtIndex(index: Int, elem: String): Unit = {
    _lineArrayBuffer.insert(index, elem)
  }

  def dropElemAtIndex(index: Int): Unit = {
    _lineArrayBuffer.remove(index)
  }

  def replaceElemAtIndex(index: Int, elem: String): Unit = {
    _lineArrayBuffer.remove(index)
    _lineArrayBuffer.insert(index, elem)
  }

  def getAsArray: Array[String] = _lineArrayBuffer.toArray
  def getAsString: String = _lineArrayBuffer.mkString("|")
}

object CSVIOTools {
  def estLineCount(file: File): Long = {
    val fileSize: Long = file.length() // count by Byte
    if (fileSize == 0) {
      return 0
    }
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