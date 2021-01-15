package cn.pandadb.tools.importer

import java.io.File

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:41 2021/1/14
 * @Modified By:
 */
case class ImportCmd(args: Array[String]) {
  val funcName: String = args(0)

  val nodeFileList: List[File] = {
    val nodesFilesPath: Array[String] = _getArgByName("nodes").split(",")
    nodesFilesPath.map(filePath => new File(filePath)).toList
  }
  val relFileList: List[File] = {
    val relsFilesPath: Array[String] = _getArgByName("relationships").split(",")
    relsFilesPath.map(filePath => new File(filePath)).toList
  }
  val exportDBPath: File = {
    val dbFile = new File(_getArgByName("db-path"))
    if (!dbFile.isDirectory || dbFile.listFiles().length != 0) {
      throw new Exception(s"The export db path $dbFile is not an empty directory.")
    }
    dbFile
  }

  val delimeter: String = {
    val delimeter = _getArgByName("delimeter")
    if(delimeter.length>1) throw new Exception(s"The delimeter takes only one character, modify your input $delimeter please.")
    if(delimeter.length == 1) delimeter
    else ","
  }

  val arrayDelimeter: String = {
    val arrayDelimeter: String = _getArgByName("array-delimeter")
    if (arrayDelimeter.length>1) throw new Exception(s"The array-delimeter takes only one character, modify your input $arrayDelimeter please.")
    if(arrayDelimeter.length == 1) arrayDelimeter
    else "|"
  }

  private def _getArgByName(name: String): String = {
    val index: Int = args.indexWhere(arg => arg.startsWith(s"--$name="))
    if (index == -1) ""
    else args(index).replace("--$name=", "")
  }

}
