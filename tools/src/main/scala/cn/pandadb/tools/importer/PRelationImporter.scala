package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBGraphAPI, RocksDBStorage}
import cn.pandadb.kernel.kv.relation.{RelationDirection, RelationDirectionStore, RelationLabelIndex, RelationPropertyStore}
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
// TODO 1.relationLabel 2.relationType String->Int

/**
 *
 * protocol: :relId(long), :fromId(long), :toId(long), :edgetype(string), category(Int), propName1:type, ...
 */
class PRelationImporter(dbPath: String, edgeFile: File, hFile: File) extends Importer {
  val file: File = edgeFile
  val headFile: File = hFile
  var propSortArr: Array[Int] = null
  val headMap: Map[Int, String] = _setEdgeHead()
  val relationDB = RocksDBStorage.getDB(s"${dbPath}/rels")
  val relationStore = new RelationPropertyStore(relationDB)
  val inRelationDB = RocksDBStorage.getDB(s"${dbPath}/inEdge")
  val inRelationStore = new RelationDirectionStore(inRelationDB, RelationDirection.IN)
  val outRelationDB = RocksDBStorage.getDB(s"${dbPath}/outEdge")
  val outRelationStore = new RelationDirectionStore(outRelationDB, RelationDirection.OUT)
  val relationLabelDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex")
  val relationLabelStore = new RelationLabelIndex(relationLabelDB)
  val relSerializer = RelationSerializer

  def importEdges(): Unit ={
    val estEdgeCount: Long = estLineCount(file)
    val iter = Source.fromFile(edgeFile).getLines()
    var i = 0

    val inWriteOpt = new WriteOptions()
    val inBatch = new WriteBatch()

    val outWriteOpt = new WriteOptions()
    val outBatch = new WriteBatch()

    val storeWriteOpt = new WriteOptions()
    val storeBatch = new WriteBatch()

    while (iter.hasNext) {
      if(i%10000000 == 0){
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i/10000000}kw of $estEdgeCount(est) edges imported. $time1")
      }
      i += 1
      val relation = _wrapEdge(iter.next().replace("\n", "").split(","))

      storeBatch.put(KeyHandler.relationKeyToBytes(relation.id), RelationSerializer.serialize(relation))
      inBatch.put(KeyHandler.edgeKeyToBytes(relation.to, relation.typeId, relation.from), ByteUtils.longToBytes(relation.id))
      outBatch.put(KeyHandler.edgeKeyToBytes(relation.from, relation.typeId, relation.to), ByteUtils.longToBytes(relation.id))
      if(i%1000000 == 0) {
        relationDB.write(storeWriteOpt, storeBatch)
        inRelationDB.write(inWriteOpt, inBatch)
        outRelationDB.write(outWriteOpt, outBatch)
        storeBatch.clear()
        inBatch.clear()
        outBatch.clear()
      }
    }
    relationDB.write(storeWriteOpt, storeBatch)
    inRelationDB.write(inWriteOpt, inBatch)
    outRelationDB.write(outWriteOpt, outBatch)
    storeBatch.clear()
    inBatch.clear()
    outBatch.clear()
//    PDBMetaData.persist(rocksDBGraphAPI.getMetaDB)
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

  private def _wrapEdge(lineArr: Array[String]): StoredRelationWithProperty = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))

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
    new StoredRelationWithProperty(relId, fromId, toId, edgeType, propMap)
  }

}
