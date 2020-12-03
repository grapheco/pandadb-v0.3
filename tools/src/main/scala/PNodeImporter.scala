import java.io.File

import cn.pandadb.kernel.kv.NodeStore
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
  headMap(propId1 -> type, propId2 -> type ...)
 */


// protocol: :ID :LABELS propName1:type1 proName2:type2
case class TempNode(id: Long, labels: Array[Int], properties: Map[Int, Any])

class PNodeImporter(nodeFile: File, rocksDB: RocksDB, hFile: File) extends PandaImporter {
  override val db: RocksDB = rocksDB
  override val file: File = nodeFile
  override val headFile: File = hFile
  override val headMap: Map[Int, String] = _setHead()
  val nodeStore = new NodeStore(db)

  // record the propId sort in the file, example: Array(2, 4, 1, 3)
  var propSortArr: Array[Int] = null

//  def importNodes() = {
//    val iter = Source.fromFile(file).getLines()
//    while (iter.hasNext) {
//      val lineArray = iter.next().replace("\n", "").split(",")
//      val tempNode = _wrapNode(lineArray)
//      nodeStore.set(tempNode.id, tempNode.labels, tempNode.properties)
//    }
//  }

  private def _setHead(): Map[Int, String] = {
    var hMap: Map[Int, String] = Map[Int, String]()
    val headArr = Source.fromFile(hFile).getLines().next().replace("\n", "").split(",")
    propSortArr = Array[Int](headArr.length)
    // headArr(0) is :ID, headArr(1) is :LABELS
    for(i <- 2 until headArr.length - 1) {
      val fieldArr = headArr(i).split(":")
      val propId: Int = PDBMetaData.getPropId(fieldArr(0))
      propSortArr(i-2) = propId
      val propType: String = fieldArr(1).toLowerCase()
      hMap += (propId -> propType)
    }
    hMap
  }

  private def _wrapNode(lineArr: Array[String]): TempNode = {
    val id = lineArr(0).toLong
//    TODO：modify the labels import mechanism, enable real array
    val labels: Array[Int] = Array(lineArr(1).toInt)
    var propMap: Map[Int, Any] = Map[Int, Any]()
    for(i <-2 until lineArr.length -1) {
      val propId = propSortArr(i - 2)
      val propValue: Any = {
        headMap(propId) match {
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i)
        }
      }
      propMap += (propId -> propValue)
    }
    TempNode(id, labels, propMap)
  }



}















//  private def _wrapNode(arr: Array[String]): TempNode = {
//    val id: Long = arr(headMap.get(":ID").get.asInstanceOf[Int]).toLong
//    val labels: Array[Int] = {
//      val labelIndexArr: Array[Int] = headMap.get(":LABEL").get.asInstanceOf[Array[Int]]
//      labelIndexArr.map(item => arr(item).toInt)
//    }
//    val props: Map[String, Any]
//    val tNode =
//  }



//  def setHead(): Map[String, Any] = {
//    val headArr = Source.fromFile(file).getLines().next().replace("\n", "").split(",")
//    var headMap: mutable.Map[String, Any] = mutable.Map[String, Any]()
//    headArr.foreach(item => {
//      item match {
//        case _ if item.endsWith(":ID") => {
//          if(headMap.contains(":ID")) throw new Exception("Repeat define of :ID in the head file.")
//          headMap += (":ID" -> headArr.indexOf(item))
//        }
//        case _ if item.endsWith(":LABEL") => {
//          if (headMap.contains(":LABEL")) headMap(":LABEL") = concat(headMap(":LABEL").asInstanceOf[Array[Int]], Array(headArr.indexOf(item)))
//          else headMap += (":LABEL" -> Array(headArr.indexOf(item)))
//        }
//        case _ => headMap += (item -> headArr.indexOf(item))
//      }
//    })
//    headMap.toMap
//  }


