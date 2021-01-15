package cn.pandadb.tools.importer

import java.io.File

import cn.pandadb.kernel.PDBMetaData
import org.apache.logging.log4j.scala.Logging

import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:20 2020/12/9
 * @Modified By:
 */
object PandaImporter extends Logging{

//  val stdNodeHeadPrefix: Array[String] = Array(":ID", ":LABEL")
//  val stdRelationHeadPrefix: Array[String] = Array(":REL_ID", ":FROMID", ":TOID", ":TYPE")

  def main(args: Array[String]): Unit = {
    // args: dbPath, nodeHead file path, node file path, relHead file path, relation file path
//    _argsCheck(args)
    val nodeImporter = new PNodeImporter(args(0), new File(args(1)), new File(args(2)))
    val relationImporter = new PRelationImporter(args(0), new File(args(3)), new File(args(4)))
    logger.info("Import task started.")
    nodeImporter.importNodes()
    relationImporter.importRelations()
    PDBMetaData.persist(args(0))
    logger.info("import task finished.")
  }

//  private def _argsCheck(args: Array[String]): Boolean = {
//    if(args.length!=5)
//      throw new Exception(s"Wrong args number. The importer needs 5 args. args: dbPath, nodeHead file path, node file path, relHead file path, relation file path")
//    if(!_isEnvAvailable(args(0)))
//      throw new Exception(s"The dbPath ${args(0)} is not empty, try an empty directory please.")
//    // files exist?
//    for(i<-1 until 5){
//      if(!new File(args(i)).exists()) throw new Exception(s"The file ${args(i)} not exists.")
//    }
//    _csvFormatCheck(new File(args(1)), new File(args(2)), stdNodeHeadPrefix)
//    _csvFormatCheck(new File(args(3)), new File(args(4)), stdRelationHeadPrefix)
//    true
//  }

  private def _isEnvAvailable(dbPath: String): Boolean = {
    val dbFile = new File(dbPath)
    val isEmptyDirectory: Boolean = dbFile.isDirectory && dbFile.listFiles().length == 0
    val notExist: Boolean = !dbFile.exists()
    isEmptyDirectory || notExist
  }

  private def _csvFormatCheck(headFile: File, dataFile: File, standardHead: Array[String]): Boolean = {
    val headArray = Source.fromFile(headFile).getLines().next().split(",").map(item => item.toUpperCase)
    val dataArray = Source.fromFile(dataFile).getLines().next().split(",")
    if(headArray.length != dataArray.length) throw new Exception(s"The column number of ${headFile.getCanonicalPath} and ${dataFile.getCanonicalPath} not match.")
    //:ID,:LABEL,id_p:Long,idStr:String,flag:Boolean,name:String
    //:REL_ID,:FROMID,:TOID,:type,fromID:long,toID:String,weight:int
    val pairArray = standardHead.zip(headArray)
    if(pairArray.length < standardHead.length) throw new Exception(s"The head file ${headFile.getCanonicalPath} doesn't contain enough elems")
    standardHead.zip(headArray).foreach(pair => if(pair._1 != pair._2) throw new Exception(s"Wrong item in head file, supposed ${pair._1}, actual ${pair._2}."))
    true
  }
}
