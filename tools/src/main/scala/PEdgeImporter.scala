import java.io.File

import cn.pandadb.kernel.kv.{RelationStore}
import org.rocksdb.RocksDB

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
class PEdgeImporter(edgeFile: File, rocksDB: RocksDB, hFile: File) {
  val db: RocksDB = rocksDB
  val file: File = edgeFile
  val headFile: File = hFile
  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setEdgeHead()
  val relStore = new RelationStore(db)

  def importEdges(): Unit ={
    val iter = Source.fromFile(edgeFile).getLines()
    while (iter.hasNext) {
      val tempEdge = _wrapEdge(iter.next().replace("\n", "").split(","))
      relStore.setRelation(tempEdge.relId, tempEdge.fromId, tempEdge.toId, tempEdge.edgeType, 1, tempEdge.propMap)
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