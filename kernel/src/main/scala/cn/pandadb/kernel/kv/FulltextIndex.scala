//package cn.pandadb.kernel.kv
//
//import cn.pandadb.kernel.{Costore, TypedId}
//import org.rocksdb.RocksDB
//
//import java.util.UUID
//
//case class FulltextIndex(db: RocksDB, indexPathPrefix: String, val labelID: Int, val propsIDs: Array[Int]) {
//
//  private var handler: Costore = null
//  private var indexID: UUID = null
//
//  private val indexMetaKey = KeyHandler.nodePropertyFulltextIndexMetaKeyToBytes(labelID, propsIDs)
//
//  def exists: Boolean ={
//    if (indexID != null){
//      return true
//    }
//    val v = db.get(indexMetaKey)
//    if (v == null || v.length!=36) {
//      return false
//    }
//    indexID = UUID.fromString(ByteUtils.stringFromBytes(v, 0))
//    true
//  }
//
//  def createAndOpen(): Unit ={
//    if (exists) {
//      throw new Exception(s"index for label {$labelID} and props ${propsIDs} already exists")
//    }
//    createIndexMeta
//    handler = new Costore(s"${indexPathPrefix}/${indexID}")
//  }
//
//  def open(forceCreate: Boolean = false): Unit ={
//    if(!forceCreate && !exists){
//      throw new Exception(s"index for label {$labelID} and props ${propsIDs} does not exists!")
//    }
//    if(!exists){
//      createAndOpen()
//    }else{
//      handler = new Costore(s"${indexPathPrefix}/${indexID}")
//    }
//  }
//
//  def dropAndClose(): Unit = {
//    handler.dropAndClose()
//    deleteIndexMeta()
//  }
//
//  def close(): Unit ={
//    handler.close()
//    indexID = null
//  }
//
//  def createIfNotExists(): Unit = {
//    if (!exists){
//      createAndOpen()
//    }
//  }
//
//  def dropIfExists(): Unit = {
//    if (exists){
//      open()
//      dropAndClose()
//    }
//  }
//
//  def insert(records: Iterator[(TypedId, Map[String, String])]): Unit ={
//    records.foreach({
//      r=>{
//        handler.insert(r._1, r._2)
//      }
//    })
//  }
//
//  def update(records: Iterator[(TypedId, Map[String, String])]): Unit ={
//    records.foreach({
//      r => {
//        handler.delete(r._1)
//        handler.insert(r._1, r._2)
//      }
//    })
//  }
//
//  def delete(recordIDs: Iterator[TypedId]): Unit ={
//    recordIDs.foreach({
//      r => handler.delete(r)
//    })
//  }
//
//  def find(keyword: (Array[String], String)): Iterator[Map[String, Any]] = {
//    handler.topDocs2NodeWithPropertiesArray(handler.search(keyword)).get.iterator
//  }
//
//  /**
//   * Index MetaData
//   * ------------------------
//   *      key      |  value
//   * ------------------------
//   * label + props |  indexId
//   * ------------------------
//   */
//  def createIndexMeta(): Unit = {
//    indexID = UUID.randomUUID()
//    db.put(indexMetaKey,ByteUtils.stringToBytes(indexID.toString))
//  }
//
//  def deleteIndexMeta(): Unit = {
//    db.delete(indexMetaKey)
//    indexID = null
//  }
//
//}
