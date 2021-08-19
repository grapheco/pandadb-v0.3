package cn.pandadb.kernel.util

import java.io.{File, FileWriter}

import cn.pandadb.kernel.transaction.DBNameMap
import com.typesafe.scalalogging.LazyLogging

/**
 * @program: pandadb-v0.3
 * @description: some utils
 * @author: LiamGao
 * @create: 2021-08-19 09:15
 */
object CommonUtils extends LazyLogging{

  def checkDir(dir: String): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      file.mkdirs()
      logger.info(s"New created data path (${dir})")
    }
    else {
      if (!file.isDirectory) {
        throw new Exception(s"The data path (${dir}) is invalid: not directory")
      }
    }
  }

  def createFile(path: String): Unit ={
    val file = new File(path)
    if (!file.exists()) file.createNewFile()
  }

  def cleanFileContent(path: String): Unit ={
    val fileWriter = new FileWriter(path)
    fileWriter.write("")
    fileWriter.flush()
    fileWriter.close()
  }
}
