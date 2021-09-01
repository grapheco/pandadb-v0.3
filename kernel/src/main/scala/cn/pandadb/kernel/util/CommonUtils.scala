package cn.pandadb.kernel.util

import java.io.{File, FileWriter}

import com.typesafe.scalalogging.LazyLogging

import scala.util.matching.Regex

/**
 * @program: pandadb-v0.3
 * @description: some utils
 * @author: LiamGao
 * @create: 2021-08-19 09:15
 */
object CommonUtils extends LazyLogging{
  val r1 = "(explain\\s+)?match\\s*\\(.*\\s*\\{?.*\\}?\\s*\\)\\s*(where)?\\s*.*\\s*(set|remove|delete|merge)\\s*"
  val r3 = "(explain\\s+)?merge\\s*\\(.*\\s*\\{?.*\\}?\\s*\\)\\s*(where)?\\s*.*\\s*(set|remove|delete|merge)?\\s*"
  val r2 = "(explain\\s+)?create\\s*\\(.*\\{?.*\\}?\\s*\\)"
  val r4 = "create index on"
  val pattern = new Regex(s"${r1}|${r2}|${r3}|${r4}")

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

  def isWriteCypher(statement: String): Boolean = {
    val cypher = statement.toLowerCase().replaceAll("\n", " ").replaceAll("\r", "")
    pattern.findAllIn(cypher).nonEmpty
  }
}
