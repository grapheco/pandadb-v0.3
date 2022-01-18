package org.grapheco.pandadb.tools.importer

import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException

import java.io.File

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:41 2021/1/14
 * @Modified By:
 */
case class ImportCmd(args: Array[String]) {
  val funcName: String = args(0)

  val kvHosts: String = {
    val res = _getArgByName("kv-hosts")
    if (res.isEmpty) throw new PandaDBException("must with 'kv-hosts' keyword")
    res
  }

//  val indexHosts: String = {
//    val res = _getArgByName("index-hosts")
//    if (res.isEmpty) throw new PandaDBException("must with 'index-hosts' keyword")
//    res
//  }

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

  private def _getArgByName(name: String, default: String): String = {
    val filtered: Array[String] = args.filter(arg => arg.startsWith(s"--$name=")).map(arg => arg.replace(s"--$name=", ""))
    if (filtered.length == 0) default
    else filtered.mkString(",")
  }
  private def _transferDelimeter(srcDelimeter: String): String = {
    val needTransfer: Boolean = srcDelimeter.equals("|")
    if (needTransfer) s"\\$srcDelimeter"
    else srcDelimeter
  }

}
