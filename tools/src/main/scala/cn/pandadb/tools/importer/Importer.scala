package cn.pandadb.tools.importer

import java.io.{File, FileInputStream}

import org.junit.Test

import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:09 2020/12/18
 * @Modified By:
 */
class Importer {

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

