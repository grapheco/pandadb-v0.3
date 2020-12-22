package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.kv.RocksDBGraphAPI

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:20 2020/12/9
 * @Modified By:
 */
object PandaImporter {
  val srcNodeFile = new File("D://GitSpace//ScalaUtils//nodes50M-wrapped.csv")
  val srcEdgeFile = new File("D://GitSpace//ScalaUtils//edges50M-wrapped.csv")
  val headNodeFile = new File("G://dataset//nodes-1k-wrapped-head.csv")
  val headEdgeFile = new File("G://dataset//edges-1k-wrapped-head.csv")

  val dbPath = "C:\\PandaDB\\base_50M"
  val rocksDBGraphAPI = new RocksDBGraphAPI(dbPath)

  def main(args: Array[String]): Unit = {
    val nodeImporter = new PNodeImporter(dbPath, srcNodeFile, headNodeFile)
    val edgeImporter = new PRelationImporter(dbPath, srcEdgeFile, headEdgeFile)
    val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(time1)
    nodeImporter.importNodes()
    val time2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(time2)
//    edgeImporter.importEdges()
    val time3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(time3)
  }

}
