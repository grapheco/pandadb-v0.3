package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBStorage}

/**
 * @ClassName IndexAPI
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/23
 * @Version 0.1
 */
class IndexStoreAPI(dbPath: String) {

  type IndexId   = Int
//  type Long    = Long

  private val metaDB = RocksDBStorage.getDB(s"${dbPath}/indexMeta")
  private val meta = new IndexMetaData(metaDB)
  private val indexDB = RocksDBStorage.getDB(s"${dbPath}/index")
  private val index = new IndexStore(indexDB)

  def createIndex(label: Int, props: Array[Int]): IndexId = meta.addIndexMeta(label, props)

  def createIndex(label: Int, prop: Int): IndexId = meta.addIndexMeta(label, Array(prop))

  def getIndexId(label: Int, props: Array[Int]): Option[IndexId] = meta.getIndexId(label, props)

  def getIndexIdByLabel(label: Int): Array[(Array[Int], IndexId)] = meta.getIndexId(label)

  def allIndexId: Iterator[IndexId] = meta.all()

  def insertIndexRecord(indexId: IndexId, data: Any, id: Long): Unit = {
    index.set(indexId, IndexEncoder.typeCode(data), IndexEncoder.encode(data), id)
  }

  def insertIndexRecordBatch(indexId: IndexId, data: Iterator[(Any, Long)]): Unit =
    index.set(indexId, data)

  def updateIndexRecord(indexId: IndexId, value: Any, id: Long, newValue: Any): Unit = {
    index.update(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value),
      id, IndexEncoder.typeCode(newValue), IndexEncoder.encode(newValue))
  }

  def deleteIndexRecord(indexId: IndexId, value: Any, id: Long): Unit ={
    index.delete(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value), id)
  }

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    meta.getIndexId(label, props).foreach{
      id=>
        index.deleteRange(id)
        meta.deleteIndexMeta(label, props)
    }
  }

  def findByPrefix(prefix: Array[Byte]): Iterator[Long] = {
    val iter = indexDB.newIterator()
    iter.seek(prefix)
    new Iterator[Long] (){
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix)

      override def next(): Long = {
        val key = iter.key()
        val id = ByteUtils.getLong(key, key.length-8)
        iter.next()
        id
      }
    }
  }

  def find(indexId: IndexId, value: Any): Iterator[Long] =
    findByPrefix(KeyHandler.nodePropertyIndexPrefixToBytes(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value)))

  private def findRange(indexId:IndexId, valueType: Byte, startValue: Array[Byte] , endValue: Array[Byte]): Iterator[Long] = {
    val typePrefix  = KeyHandler.nodePropertyIndexTypePrefix(indexId, valueType)
    val startPrefix = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, valueType, startValue)
    val endPrefix   = KeyHandler.nodePropertyIndexPrefixToBytes(indexId, valueType, endValue)
    val iter = indexDB.newIterator()
    iter.seekForPrev(endPrefix)
    val endKey = iter.key()
    iter.seek(startPrefix)
    new Iterator[Long] (){
      var end = false
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(typePrefix) && !end
      override def next(): Long = {
        val key = iter.key()
        end = key.startsWith(endKey)
        val id = ByteUtils.getLong(key, key.length-8)
        iter.next()
        id
      }
    }
  }


  def findStringStartWith(indexId: IndexId, string: String): Iterator[Long] =
    findByPrefix(
      KeyHandler.nodePropertyIndexPrefixToBytes(
        indexId,
        IndexEncoder.STRING_CODE,
        IndexEncoder.encode(string).take(string.getBytes().length / 8 + string.getBytes().length)))


  def findIntRange(indexId: IndexId, startValue: Int = Int.MinValue, endValue: Int = Int.MaxValue): Iterator[Long] =
    findRange(indexId, IndexEncoder.INT_CODE, IndexEncoder.encode(startValue), IndexEncoder.encode(endValue))

  def findFloatRange(indexId: IndexId, startValue: Float, endValue: Float): Iterator[Long] =
    findRange(indexId, IndexEncoder.FLOAT_CODE, IndexEncoder.encode(startValue), IndexEncoder.encode(endValue))

  def close(): Unit = {
    indexDB.close()
    metaDB.close()
  }
}
