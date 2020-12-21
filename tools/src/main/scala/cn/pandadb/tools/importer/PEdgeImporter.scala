package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.RocksDBGraphAPI
import org.rocksdb.{WriteBatch, WriteOptions}

import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:01 2020/12/4
 * @Modified By:
 */

/**
 *
 * protocol: :relId(long), :fromId(long), :toId(long), :edgetype(string), propName1:type, ...
 */
case class TempEdge(relId: Long, fromId: Long, toId: Long, edgeType: Int, propMap: Map[String, Any])
class PEdgeImporter(edgeFile: File, hFile: File, rocksDBGraphAPI: RocksDBGraphAPI) extends Importer {
  val file: File = edgeFile
  val headFile: File = hFile
  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setEdgeHead()

  def importEdges(): Unit ={
    val estEdgeCount: Long = estLineCount(file)
    val iter = Source.fromFile(edgeFile).getLines()
    var i = 0

    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()

    while (iter.hasNext) {
      if(i%10000000 == 0){
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i/10000000}kw of $estEdgeCount(est) edges imported. $time1")
      }
      i += 1
      val tempEdge = _wrapEdge(iter.next().replace("\n", "").split(","))
      rocksDBGraphAPI.addRelation(tempEdge.relId, tempEdge.fromId, tempEdge.toId, tempEdge.edgeType, tempEdge.propMap)
    }
  }

  private def _setEdgeHead(): Map[String, String] = {
    var hMap: Map[String, String] = Map[String, String]()
    val headArr = Source.fromFile(hFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[String](headArr.length-4)
    // headArr[]: fromId, toId, edgetype, propName1:type, ...
    for(i <- 4 to headArr.length - 1) {
      val fieldArr = headArr(i).split(":")
      val propName: String = fieldArr(0)
      propSortArr(i - 4) = propName
      val propType: String = {
        if(fieldArr.length == 1) "string"
        else fieldArr(1).toLowerCase()
      }
      hMap += (propName -> propType)
    }
    hMap
  }

  private def _wrapEdge(lineArr: Array[String]): TempEdge = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))

    var propMap: Map[String, Any] = Map[String, Any]()
    for(i <-4 to lineArr.length -1) {
      val propName = propSortArr(i - 4)
      val propValue: Any = {
        headMap(propName) match {
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += (propName -> propValue)
    }
    TempEdge(relId, fromId, toId, edgeType, propMap)
  }

}
