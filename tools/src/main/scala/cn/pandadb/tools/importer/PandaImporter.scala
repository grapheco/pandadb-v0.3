package cn.pandadb.tools.importer

import java.io.File

import cn.pandadb.kernel.PDBMetaData
import org.apache.logging.log4j.scala.Logging

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:20 2020/12/9
 * @Modified By:
 */
object PandaImporter extends Logging{

//  val srcNodeFile = new File("D://GitSpace//ScalaUtils//nodes5kw.csv")
//  val srcNodeFile = new File("C://PandaDB//nodes5kw.csv")
//  val srcEdgeFile = new File("D://GitSpace//ScalaUtils//edges5kw.csv")
//  val headNodeFile = new File("D://GitSpace//ScalaUtils//nodeHead.csv")
//  val headEdgeFile = new File("D://GitSpace//ScalaUtils//relationHead.csv")

  val srcNodeFile = new File("D://GitSpace//ScalaUtils//nodes50M-wrapped.csv")
  val srcEdgeFile = new File("D://GitSpace//ScalaUtils//edges50M-wrapped.csv")
  val headNodeFile = new File("G:\\dataset/nodes-1k-wrapped-head.csv")
  val headEdgeFile = new File("G:\\dataset/edges-1k-wrapped-head.csv")
  val dbPath = "C:\\PandaDB\\base_50M"

  def main(args: Array[String]): Unit = {
    if(!isEnvAvailable(dbPath)) throw new Exception(s"The dbPath $dbPath is not empty, try an empty directory please.")
    val nodeImporter = new PNodeImporter(dbPath, srcNodeFile, headNodeFile)
    val edgeImporter = new PRelationImporter(dbPath, srcEdgeFile, headEdgeFile)
    logger.info("Import task started.")
    nodeImporter.importNodes()
    edgeImporter.importRelations()
    PDBMetaData.persist(dbPath)
    logger.info("import task finished.")
  }

  def isEnvAvailable(dbPath: String): Boolean = {
    val dbFile = new File(dbPath)
    val isEmptyDirectory: Boolean = dbFile.isDirectory && dbFile.listFiles().length == 0
    val notExist: Boolean = !dbFile.exists()
    isEmptyDirectory || notExist
  }

}
