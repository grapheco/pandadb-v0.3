package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.node.NodeValue
import cn.pandadb.kernel.kv.{KeyHandler, RocksDBGraphAPI}
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.rocksdb.{WriteBatch, WriteOptions}

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
class PNodeImporter(nodeFile: File, hFile : File, rocksDBGraphAPI: RocksDBGraphAPI) extends Importer {

  val file: File = nodeFile
  val headFile: File = hFile

  // record the propId sort in the file, example: Array(2, 4, 1, 3)
  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setNodeHead()

  def importNodes(): Unit = {
    val estNodeCount: Long = estLineCount(nodeFile)
    val iter = Source.fromFile(file).getLines()
    var i = 0

    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()
    val nodeValueSerializer = NodeSerializer

    while (iter.hasNext) {
      if(i % 10000000 == 0) {
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i/10000000}kw of $estNodeCount(est) nodes imported. $time1")
      }
      i += 1;
      val lineArr = iter.next().replace("\n", "").split(",")
      val nodeKey = KeyHandler.nodeKeyToBytes(lineArr(0).toLong)
      val nodeValue = _wrapNode(lineArr)
      val serializedNodeValue = nodeValueSerializer.serialize(nodeValue)
      batch.put(nodeKey, serializedNodeValue)
      if(i%1000000 ==0) {
        rocksDBGraphAPI.getNodeStoreDB.write(writeOpt, batch)
        batch.clear()
      }

    }
  }

  private def _setNodeHead(): Map[String, String] = {
    //head map: propName -> type
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

  private def _wrapNode(lineArr: Array[String]): NodeValue = {
    val id = lineArr(0).toLong
    val labels: Array[String] = lineArr(1).split(";")
    val labelIds: Array[Int] = labels.map(label => PDBMetaData.getLabelId(label))
    var propMap: Map[Int, Any] = Map[Int, Any]()
    for(i <-2 to lineArr.length -1) {
      val propName = propSortArr(i - 2)
      val propValue: Any = {
        headMap(propName) match {
          case "float" => lineArr(i).toFloat
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += (PDBMetaData.getPropId(propName) -> propValue)
    }
    new NodeValue(id, labelIds, propMap)
  }

}