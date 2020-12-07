package cn.pandadb.kernel.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import cn.pandadb.kernel.kv.NodeIndex.{IndexId, metaIdKey}
import cn.pandadb.kernel.{Costore, TypedId}
import org.rocksdb.RocksDB

import scala.util.Random

class NodeFulltextIndex(val db: RocksDB, val indexPath: String){
  
  type IndexId   = Int
  type NodeId    = Long

  val metaIdKey = Array[Byte](KeyType.NodePropertyFulltextIndexMeta.id.toByte)

  /**
   * Index MetaData
   * ------------------------
   *      key      |  value
   * ------------------------
   * label + props |  indexId
   * ------------------------
   */
  def addIndexMeta(label: Int, props: Array[Int]): IndexId = {
    val key = KeyHandler.nodePropertyFulltextIndexMetaKeyToBytes(label, props)
    val id  = db.get(key)
    if (id == null || id.isEmpty){
      val new_id = getIncreasingId()
      val id_byte = new Array[Byte](4)
      ByteUtils.setInt(id_byte, 0, new_id)
      db.put(key,id_byte)
      new_id
    } else {
      // exist
      ByteUtils.getInt(id, 0)
    }
  }


  def getIncreasingId(): IndexId ={
    val increasingId = db.get(metaIdKey)
    val id_bytes = new Array[Byte](4)
    if (increasingId == null || increasingId.length == 0){
      val id:Int = 0
      ByteUtils.setInt(id_bytes,0, id+1)
      db.put(metaIdKey, id_bytes)
      id
    } else {
      val id:Int = ByteBuffer.wrap(increasingId).getInt(0)
      ByteUtils.setInt(id_bytes,0, id+1)
      db.put(metaIdKey, id_bytes)
      id
    }
  }

  def deleteIndexMeta(label: Int, props: Array[Int]): Unit = {
    db.delete(KeyHandler.nodePropertyFulltextIndexMetaKeyToBytes(label, props))
  }

  def getIndexId(label: Int, props: Array[Int]): IndexId = {
    val v = db.get(KeyHandler.nodePropertyFulltextIndexMetaKeyToBytes(label, props))
    if (v == null || v.length < 4) {
      -1
    }else{
      ByteUtils.getInt(v, 0)
    }
  }

  /**
   * Index
   */
  def createIndex(label: Int, props: Array[Int]): IndexId = {
    addIndexMeta(label, props)
  }

  private def genIndexFullPath(label: Int, props: Array[Int]): String ={
    genIndexFullPath(getIndexId(label, props))
  }

  private def genIndexFullPath(indexId: IndexId): String ={
    s"${indexPath}/${indexId}"
  }

  def insertIndexRecord(indexId: IndexId, data: Iterator[(TypedId, Map[String, String])]): Unit ={
    val costore = new Costore(genIndexFullPath(indexId))
    data.foreach({
      d=>{
        costore.insert(d._1, d._2)
      }
    })
    costore.close()
  }

  def updateIndexRecord(indexId: IndexId, data: Iterator[(TypedId, Map[String, String])]): Unit ={
    val costore = new Costore(genIndexFullPath(indexId))
    data.foreach({
      d => {
        costore.delete(d._1)
        costore.insert(d._1, d._2)
      }
    })
    costore.close()
  }

  def deleteIndexRecord(indexId: IndexId, data: Iterator[TypedId]): Unit ={
    val costore = new Costore(genIndexFullPath(indexId))
    data.foreach({
      d => costore.delete(d)
    })
    costore.close()
  }

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    val costore = new Costore(genIndexFullPath(label, props))
    costore.indexWriter.deleteAll()
    costore.close()
    deleteIndexMeta(label, props)
  }

  def find(indexId: IndexId, keyword: (Array[String], String)): Iterator[Map[String, Any]] = {
    val costore = new Costore(genIndexFullPath(indexId))
    val topDocs = costore.search(keyword)
    val res = costore.topDocs2NodeWithPropertiesArray(topDocs).get.iterator
    costore.close()
    res
  }

}
