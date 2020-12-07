package cn.pandadb.kernel.kv

import cn.pandadb.kernel.{Costore, TypedId}
import org.rocksdb.RocksDB

import java.util.UUID
import scala.util.Random

case class NodeFulltextIndex(val db: RocksDB, val indexPathPrefix: String, val labelID: Int, propsIDs: Array[Int]) {

  private var handler: Costore = null
  private var indexID: UUID = null

  private val indexMetaKey = KeyHandler.nodePropertyFulltextIndexMetaKeyToBytes(labelID, propsIDs)

  def exists: Boolean ={
    if (indexID != null){
      return true
    }
    val v = db.get(indexMetaKey)
    if (v == null || v.length < 4) {
      return false
    }
    indexID = UUID.fromString(ByteUtils.stringFromBytes(v, 0))
    true
  }

  def create(): Unit ={
    if (exists) {
      throw new Exception(s"index for label {$labelID} and props ${propsIDs} already exists")
    }
    createIndexMeta
  }

  def open(forceCreate: Boolean = false): Unit ={
    if(!forceCreate && !exists){
      throw new Exception(s"index for label {$labelID} and props ${propsIDs} does not exists!")
    }
    if(!exists){
      create()
    }
    handler = new Costore(s"${indexPathPrefix}/${indexID}")
  }

  def drop(): Unit = {
    handler.indexWriter.deleteAll()
    deleteIndexMeta()
  }

  def close(): Unit ={
    handler.close()
    indexID = null
  }

  def createIfNotExists(): Unit = {
    if (!exists){
      create()
    }
  }

  def dropIfExists(): Unit = {
    if (exists){
      open()
      drop()
      close()
    }
  }

  def insert(records: Iterator[(TypedId, Map[String, String])]): Unit ={
    records.foreach({
      r=>{
        handler.insert(r._1, r._2)
      }
    })
  }

  def update(records: Iterator[(TypedId, Map[String, String])]): Unit ={
    records.foreach({
      r => {
        handler.delete(r._1)
        handler.insert(r._1, r._2)
      }
    })
  }

  def delete(recordIDs: Iterator[TypedId]): Unit ={
    recordIDs.foreach({
      r => handler.delete(r)
    })
  }

  def find(keyword: (Array[String], String)): Iterator[Map[String, Any]] = {
    handler.topDocs2NodeWithPropertiesArray(handler.search(keyword)).get.iterator
  }

  /**
   * Index MetaData
   * ------------------------
   *      key      |  value
   * ------------------------
   * label + props |  indexId
   * ------------------------
   */
  def createIndexMeta(): Unit = {
    indexID = UUID.randomUUID()
    db.put(indexMetaKey,ByteUtils.stringToBytes(indexID.toString))
  }

  def deleteIndexMeta(): Unit = {
    db.delete(indexMetaKey)
    indexID = null
  }

}
