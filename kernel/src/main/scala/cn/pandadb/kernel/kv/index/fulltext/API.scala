package cn.pandadb.kernel.kv.index.fulltext

case class API(dbPath: String, indexID: Int) {

  private var handler: Store = new Store(s"${dbPath}/lucene/${indexID}")

//  def exists: Boolean ={
//    if (indexID != null){
//      return true
//    }
//    val v = db.get(indexMetaKey)
//    meta.getIndexId(label, props)
//    if (v == null || v.length!=36) {
//      return false
//    }
//    indexID = ???
//    true
//  }
//
//  def createAndOpen(): Unit ={
//    if (exists) {
//      throw new Exception(s"index ${indexID} already exists")
//    }
//    createIndexMeta
//    handler = new Store(s"${indexPathPrefix}/${indexID}")
//  }

//  def open(forceCreate: Boolean = false): Unit ={
//    if(!forceCreate && !exists){
//      throw new Exception(s"index for label {$labelID} and props ${propsIDs} does not exists!")
//    }
//    if(!exists){
//      createAndOpen()
//    }else{
//      handler = new Store(s"${indexPathPrefix}/${indexID}")
//    }
//  }

  def dropAndClose(): Unit = {
    handler.dropAndClose()
//    deleteIndexMeta()
  }

  def close(): Unit ={
    handler.close()
//    indexID = null
  }

//  def createIfNotExists(): Unit = {
//    if (!exists){
//      createAndOpen()
//    }
//  }

//  def dropIfExists(): Unit = {
//    if (exists){
//      open()
//      dropAndClose()
//    }
//  }

  def insertIndexRecord(data: Map[String, Any], id: Long): Unit = {
    handler.insert(id, data)
  }

  def insertIndexRecordBatch(data: Iterator[(Map[String, Any], Long)]): Unit =
    data.foreach(d => {
      handler.insert(d._2, d._1)
    })

  def updateIndexRecord(id: Long, newValue: Map[String, Any]): Unit = {
    handler.delete(id)
    handler.insert(id, newValue)
  }

  def deleteIndexRecord(id: Long): Unit ={
    handler.delete(id)
  }

  def find(keyword: (Array[String], String)): Iterator[Long] = {
    handler.topDocs2NodeIdArray(handler.search(keyword)).get
  }

  /**
   * Index MetaData
   * ------------------------
   *      key      |  value
   * ------------------------
   * label + props |  indexId
   * ------------------------
   */
//  def createIndexMeta(): Unit = {
//    indexID = UUID.randomUUID()
//    db.put(indexMetaKey,ByteUtils.stringToBytes(indexID.toString))
//  }
//
//  def deleteIndexMeta(): Unit = {
//    db.delete(indexMetaKey)
//    indexID = null
//  }

}
