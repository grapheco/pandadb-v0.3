package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.meta.{IdGenerator}
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}

import scala.collection.mutable

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
  private val indexIdGenerator = new IdGenerator(metaDB, 200)

  //indexId->([name, address], Store)
  private val fulltextIndexMap = new mutable.HashMap[Int, (Array[Int], FulltextIndexStore)]()
  private val fulltextIndexPathPrefix = s"${dbPath}/index/fulltextIndex"

  def createIndex(label: Int, props: Array[Int], fulltext: Boolean = false): IndexId =
    meta.getIndexId(label, props).getOrElse{
      val id = indexIdGenerator.nextId().toInt
      meta.addIndexMeta(label, props, fulltext, id)
      id
    }

//  def createIndex(label: Int, prop: Int, fulltext: Boolean = false): IndexId = createIndex(label, Array(prop), fulltext)

  def getFulltextIndex(indexId: Int): (Array[Int], FulltextIndexStore) = {
    fulltextIndexMap.get(indexId).getOrElse{
      val props = meta.getIndexMeta(indexId).get.props.toArray[Int]
      val store = new FulltextIndexStore(s"$fulltextIndexPathPrefix/$indexId")
      fulltextIndexMap +=indexId->(props, store)
      (props, store)
    }
  }

  def getIndexId(label: Int, props: Array[Int]): Option[IndexId] = meta.getIndexId(label, props)

  def getIndexIdByLabel(label: Int): Set[IndexMeta] = meta.getIndexId(label)

  def allIndexId: Iterator[IndexMeta] = meta.all()

  def insertIndexRecord(indexId: IndexId, data: Any, id: Long): Unit = {
    index.set(indexId, IndexEncoder.typeCode(data), IndexEncoder.encode(data), id)
  }

  def insertIndexRecordBatch(indexId: IndexId, data: Iterator[(Any, Long)]): Unit =
    index.set(indexId, data)

  //data: (prop1Value, Prop2Value)
  def insertFulltextIndexRecord(indexId: IndexId, data: Array[Any], id: Long): Unit = {
    val (propIds, store) = getFulltextIndex(indexId)
    store.insert(id, propIds.zip(data).toMap.map(p => {s"${p._1}" -> p._2.asInstanceOf[String]}))
    store.commit()
  }

  def insertFulltextIndexRecordBatch(indexId: IndexId, data: Iterator[(Array[Any], Long)]): Unit = {
    val (propIds, store) = getFulltextIndex(indexId)
    data.foreach(d => {
      store.insert(d._2, propIds.zip(d._1).toMap.map(p => {s"${p._1}" -> p._2.asInstanceOf[String]}), maxBufferedDocs = 102400)
    })
    store.commit()
  }

  def updateIndexRecord(indexId: IndexId, value: Any, id: Long, newValue: Any): Unit = {
    index.update(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value),
      id, IndexEncoder.typeCode(newValue), IndexEncoder.encode(newValue))
  }

  def updateFulltextIndexRecord(indexId: IndexId, value: Any, id: Long, newValue: Array[Any]): Unit = {
    val (propIds, store) = getFulltextIndex(indexId)
    store.delete(id)
    store.insert(id, propIds.zip(newValue).toMap.map(p => {s"${p._1}" -> p._2.asInstanceOf[String]}))
  }

  def deleteIndexRecord(indexId: IndexId, value: Any, id: Long): Unit ={
    index.delete(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value), id)
  }

  def deleteFulltextIndexRecord(indexId: IndexId, value: Any, id: Long): Unit =
    getFulltextIndex(indexId)._2.delete(id)

  def dropIndex(label: Int, props: Array[Int]): Unit = {
    meta.getIndexId(label, props).foreach{
      id=>
        index.deleteRange(id)
        meta.deleteIndexMeta(label, props)
    }
  }

  def dropFulltextIndex(label: Int, props: Array[Int]): Unit = {
    meta.getIndexId(label, props, true).foreach{
      id=>
        meta.deleteIndexMeta(label, props, true)
        new FulltextIndexStore(s"$fulltextIndexPathPrefix/$id").dropAndClose()
        fulltextIndexMap -= id
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
    findByPrefix(KeyConverter.toIndexKey(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value)))

  private def findRange(indexId:IndexId,
                        valueType: Byte,
                        startValue: Array[Byte],
                        endValue: Array[Byte],
                        startClosed: Boolean,
                        endClosed: Boolean): Iterator[Long] = {
    val startTail = if(startClosed) 0 else -1
    val endTail = if(endClosed) -1 else 0
    val typePrefix  = KeyConverter.toIndexKey(indexId, valueType)
    val startPrefix = KeyConverter.toIndexKey(indexId, valueType, startValue, startTail.toLong)
    val endPrefix   = KeyConverter.toIndexKey(indexId, valueType, endValue, endTail.toLong)
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
      KeyConverter.toIndexKey(
        indexId,
        IndexEncoder.STRING_CODE,
        IndexEncoder.encode(string).take(string.getBytes().length / 8 + string.getBytes().length)))

  def search(indexId: IndexId, props: Array[Int], keyword: String): Iterator[Long] = {
    val store = getFulltextIndex(indexId)._2
    store.topDocs2NodeIdArray(store.search(props.map(v=>s"$v"),keyword))
  }

  def findIntegerRange(indexId: IndexId,
                   startValue: Long = Long.MinValue,
                   endValue: Long = Long.MaxValue,
                   startClosed: Boolean = false,
                   endClosed: Boolean = false): Iterator[Long] =
    findRange(indexId, IndexEncoder.INTEGER_CODE, IndexEncoder.encode(startValue), IndexEncoder.encode(endValue), startClosed, endClosed)

  def findFloatRange(indexId: IndexId,
                     startValue: Double = Double.MinValue,
                     endValue: Double = Double.MinValue,
                     startClosed: Boolean = false,
                     endClosed: Boolean = false): Iterator[Long] =
    findRange(indexId, IndexEncoder.FLOAT_CODE, IndexEncoder.encode(startValue), IndexEncoder.encode(endValue), startClosed, endClosed)

  def close(): Unit = {
    indexDB.close()
    metaDB.close()
    fulltextIndexMap.foreach(p=>p._2._2.close())
  }
}
