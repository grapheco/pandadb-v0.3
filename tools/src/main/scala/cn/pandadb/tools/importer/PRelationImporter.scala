package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.RocksDBGraphAPI
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:01 2020/12/4
 * @Modified By:
 */

/**
 *
 * protocol: :relId(long), :fromId(long), :toId(long), :edgetype(string), category(Int), propName1:type, ...
 */
class PRelationImporter(edgeFile: File, hFile: File, rocksDBGraphAPI: RocksDBGraphAPI) extends Importer {
  val file: File = edgeFile
  val headFile: File = hFile
  var propSortArr: Array[Int] = null
  val headMap: Map[Int, String] = _setEdgeHead()
  val inRelationDB: RocksDB = rocksDBGraphAPI.getInEdgeStoreDB
  val outRelationDB: RocksDB = rocksDBGraphAPI.getOutEdgeStoreDB
  val relSerializer = RelationSerializer

  def importEdges(): Unit ={
    val estEdgeCount: Long = estLineCount(file)
    val iter = Source.fromFile(edgeFile).getLines()
    var i = 0

    val inWriteOpt = new WriteOptions()
    val inBatch = new WriteBatch()

    val outWriteOpt = new WriteOptions()
    val outBatch = new WriteBatch()

    while (iter.hasNext) {
      if(i%10000000 == 0){
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i/10000000}kw of $estEdgeCount(est) edges imported. $time1")
      }
      i += 1
      val tempEdgePair = _wrapEdge(iter.next().replace("\n", "").split(","))
      val serializedOutRel: Array[Byte] = relSerializer.serialize(tempEdgePair._1)
      val serializedInRel: Array[Byte] = relSerializer.serialize(tempEdgePair._2)
      inBatch.put(serializedInRel, Array[Byte](0))
      outBatch.put(serializedOutRel, Array[Byte](0))
      if(i%1000000 == 0) {
        inRelationDB.write(inWriteOpt, inBatch)
        outRelationDB.write(outWriteOpt, outBatch)
        inBatch.clear()
        outBatch.clear()
      }
    }
    inRelationDB.write(inWriteOpt, inBatch)
    outRelationDB.write(outWriteOpt, outBatch)
    inBatch.clear()
    outBatch.clear()
    PDBMetaData.persist(rocksDBGraphAPI.getMetaDB)
  }

  private def _setEdgeHead(): Map[Int, String] = {
    var hMap: Map[Int, String] = Map[Int, String]()
    val headArr = Source.fromFile(hFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[Int](headArr.length-5)
    // headArr[]: fromId, toId, edgetype, propName1:type, ...
    for(i <- 5 to headArr.length - 1) {
      val fieldArr = headArr(i).split(":")
      val propId: Int = PDBMetaData.getPropId(fieldArr(0))
      propSortArr(i - 5) = propId
      val propType: String = {
        if(fieldArr.length == 1) "string"
        else fieldArr(1).toLowerCase()
      }
      hMap += (propId -> propType)
    }
    hMap
  }

  private def _wrapEdge(lineArr: Array[String]): (StoredRelationWithProperty, StoredRelationWithProperty) = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))
    val category: Int = lineArr(4).toInt

    var propMap: Map[Int, Any] = Map[Int, Any]()
    for(i <-5 to lineArr.length -1) {
      val propId: Int = propSortArr(i - 5)
      val propValue: Any = {
        headMap(propId) match {
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += (propId -> propValue)
    }
    (new StoredRelationWithProperty(relId, fromId, toId, edgeType, category, propMap),
      new StoredRelationWithProperty(relId, toId, fromId, edgeType, category, propMap))
  }

}
