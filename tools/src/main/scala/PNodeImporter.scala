import java.io.File

import cn.pandadb.kernel.kv.{NodeStore, RocksDBGraphAPI}
import org.rocksdb.RocksDB

import scala.Array.concat
import scala.collection.{SortedMap, mutable}
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 17:22 2020/12/3
 * @Modified By:
 */

/**
 *
  headMap(propName1 -> type, propName2 -> type ...)
 */
// protocol: :ID :LABELS propName1:type1 proName2:type2
case class TempNode(id: Long, labels: Array[Int], properties: Map[String, Any])

class PNodeImporter(nodeFile: File, hFile : File, rocksDBGraphAPI: RocksDBGraphAPI) {
//  val db: RocksDB = rocksDB
  val file: File = nodeFile
  val headFile: File = hFile
  // record the propId sort in the file, example: Array(2, 4, 1, 3)
  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setNodeHead()

  def importNodes(): Unit = {
    val iter = Source.fromFile(file).getLines()
    while (iter.hasNext) {
      val tempNode = _wrapNode(iter.next().replace("\n", "").split(","))
      rocksDBGraphAPI.addNode(tempNode.id, tempNode.labels, tempNode.properties)
    }
  }

  private def _setNodeHead(): Map[String, String] = {
    var hMap: Map[String, String] = Map[String, String]()
    val headArr = Source.fromFile(hFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[String](headArr.length-2)
    // headArr(0) is :ID, headArr(1) is :LABELS
    for(i <- 2 to headArr.length - 1) {
      val fieldArr = headArr(i).split(":")
      val propName: String = fieldArr(0)
      propSortArr(i-2) = propName
      val propType: String = fieldArr(1).toLowerCase()
      hMap += (propName -> propType)
    }
    hMap
  }

  private def _wrapNode(lineArr: Array[String]): TempNode = {
    val id = lineArr(0).toLong
//  TODO：modify the labels import mechanism, enable real array
    val labels: Array[Int] = Array(PDBMetaData.getLabelId(lineArr(1)))
    var propMap: Map[String, Any] = Map[String, Any]()
    for(i <-2 to lineArr.length -1) {
      val propName = propSortArr(i - 2)
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
    TempNode(id, labels, propMap)
  }

}