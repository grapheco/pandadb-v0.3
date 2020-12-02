package cn.pandadb.kernel.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.{Costore, TypedId}
import org.rocksdb.RocksDB

import scala.util.Random

class NodeFulltextIndex(db: RocksDB){
  
  type IndexId   = Int
  type NodeId    = Long

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
    if (id == null || id.length == 0){
      val new_id = Random.nextInt(100) // TODO generate
      val id_byte = new Array[Byte](4)
      ByteUtils.setInt(id_byte, 0, new_id)
      db.put(key,id_byte)
      new_id
    } else {
      // exist
      ByteUtils.getInt(id, 0)
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

  def insertIndexRecord(indexId: IndexId, data: Iterator[Map[TypedId, Map[String, String]]]): Unit ={
    val costore = new Costore(s"./testdata/output/indexfulltext-${indexId}")
    data.foreach({
      d=>{
        d.foreach({
          kv=>costore.insert(kv._1, kv._2)
        })
      }
    })
    costore.close()
  }

  def updateIndexRecord(indexId: IndexId, data: Iterator[Map[TypedId, Map[String, String]]]): Unit ={
    data.foreach({
      d => deleteIndexRecord(indexId, d.keys.iterator)
    })
    insertIndexRecord(indexId, data)
  }

  def deleteIndexRecord(indexId: IndexId, data: Iterator[TypedId]): Unit ={
    val costore = new Costore(s"./testdata/output/indexfulltext-${indexId}")
    data.foreach({
      d => costore.delete(d)
    })
    costore.close()
  }

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    val costore = new Costore(s"./testdata/output/indexfulltext-${getIndexId(label, props)}")
    costore.indexWriter.deleteAll()
    costore.close()
    deleteIndexMeta(label, props)
  }

  def find(indexId: IndexId, keyword: (Array[String], String)): Iterator[Map[String, Any]] = {
    val costore = new Costore(s"./testdata/output/indexfulltext-${indexId}")
    val topDocs = costore.search(keyword)
    costore.topDocs2NodeWithPropertiesArray(topDocs).get.iterator
  }

}
