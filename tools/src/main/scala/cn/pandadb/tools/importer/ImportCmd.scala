package cn.pandadb.tools.importer

import cn.pandadb.kernel.util.PandaDBException.PandaDBException

import java.io.File

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:41 2021/1/14
 * @Modified By:
 */
case class ImportCmd(args: Array[String]) {
  val funcName: String = args(0)
  val database: File = {
    val path = _getArgByName("db-path")
    new File(path)
  }

  val nodeFileList: List[File] = {
    val nodesFilesPath: Array[String] = _getArgByName("nodes").split(",")
    if(nodesFilesPath(0) == "") {
      throw new Exception("No node file detected.")
    } else {
      nodesFilesPath.map(filePath => new File(filePath)).toList
    }
  }
  val relFileList: List[File] = {
    val relsFilesPath: Array[String] = _getArgByName("relationships").split(",")
    if(relsFilesPath(0) == "") {
      List[File]()
    } else {
      relsFilesPath.map(filePath => new File(filePath)).toList
    }
  }
  val exportDBPath: File = {
    val dbFile = new File(_getArgByName("db-path"))
    if (!dbFile.exists()) dbFile.mkdirs()
    if (!dbFile.isDirectory || dbFile.listFiles().length != 0) {
      throw new Exception(s"The export db path ${dbFile.getAbsolutePath} is not an empty directory.")
    }
    dbFile
  }

  val nodeDBPath: String = {
    val path = _getArgByName("nodeDBPath")
    if (path == "") throw new PandaDBException("nodeDBPath is blank.")
    path
  }
  val nodeLabelDBPath: String = {
    val path = _getArgByName("nodeLabelDBPath")
    if (path == "") throw new PandaDBException("nodeLabelDBPath is blank.")
    path
  }
  val relationDBPath: String = {
    val path = _getArgByName("relationDBPath")
    if (path == "") throw new PandaDBException("relationDBPath is blank.")
    path
  }
  val inRelationDBPath: String = {
    val path = _getArgByName("inRelationDBPath")
    if (path == "") throw new PandaDBException("inRelationDBPath is blank.")
    path
  }
  val outRelationDBPath: String = {
    val path = _getArgByName("outRelationDBPath")
    if (path == "") throw new PandaDBException("outRelationDBPath is blank.")
    path
  }
  val relationTypeDBPath: String = {
    val path = _getArgByName("relationTypeDBPath")
    if (path == "") throw new PandaDBException("relationTypeDBPath is blank.")
    path
  }

  val rocksDBConfFilePath: String = {
    val confFilePath = _getArgByName("rocksConf")
    if (confFilePath.equals("")) {
      println("warning: default rocksConf used.")
      "default"
    }
    else confFilePath
  }

  val delimeter: String = {
    val delimeter = _getArgByName("delimeter")
    if(delimeter.length>1) throw new Exception(s"The delimeter takes only one character, modify your input $delimeter please.")
    if(delimeter.length == 1) _transferDelimeter(delimeter)
    else ","
  }

  val arrayDelimeter: String = {
    val arrayDelimeter: String = _getArgByName("array-delimeter")
    if (arrayDelimeter.length>1) throw new Exception(s"The array-delimeter takes only one character, modify your input $arrayDelimeter please.")
    if(arrayDelimeter.length == 1) _transferDelimeter(arrayDelimeter)
    else "|"
  }

  private def _getArgByName(name: String): String = {
    val filtered: Array[String] = args.filter(arg => arg.startsWith(s"--$name=")).map(arg => arg.replace(s"--$name=", ""))
    if (filtered.length == 0) ""
    else filtered.mkString(",")
  }

  private def _transferDelimeter(srcDelimeter: String): String = {
    val needTransfer: Boolean = srcDelimeter.equals("|")
    if (needTransfer) s"\\$srcDelimeter"
    else srcDelimeter
  }

}
