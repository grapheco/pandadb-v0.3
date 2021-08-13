package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.meta.{IdGenerator, TransactionIdGenerator}
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{Transaction, TransactionDB, WriteOptions}

import scala.collection.mutable


class TransactionIndexStoreAPI(metaDB: TransactionDB, indexDB: TransactionDB, fulltextIndexPath: String) {

  type IndexId   = Int
//  type Long    = Long

  private val meta = new TransactionIndexMetaData(metaDB)
  private val index = new TransactionIndexStore(indexDB)
  private val indexIdGenerator = new TransactionIdGenerator(metaDB, 200)

  //indexId->([name, address], Store)
  private val fulltextIndexMap = new mutable.HashMap[Int, (Array[Int], FulltextIndexStore)]()
  private val fulltextIndexPathPrefix = fulltextIndexPath


  def generateTransactions(writeOptions: WriteOptions): Map[String, Transaction] = {
    Map(DBNameMap.indexDB -> indexDB.beginTransaction(writeOptions),
      DBNameMap.indexMetaDB -> metaDB.beginTransaction(writeOptions))
  }

  def createIndex(label: Int, props: Array[Int], fulltext: Boolean = false, tx: LynxTransaction): IndexId =
    meta.getIndexId(label, props).getOrElse{
      val id = indexIdGenerator.nextId().toInt
      meta.addIndexMeta(label, props, fulltext, id, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexMetaDB))
      id
    }

//  def createIndex(label: Int, prop: Int, fulltext: Boolean = false): IndexId = createIndex(label, Array(prop), fulltext)

  def getFulltextIndex(indexId: Int): (Array[Int], FulltextIndexStore) = {
    fulltextIndexMap.getOrElse(indexId, {
      val props = meta.getIndexMeta(indexId).get.props.toArray[Int]
      val store = new FulltextIndexStore(s"$fulltextIndexPathPrefix/$indexId")
      fulltextIndexMap += indexId -> (props, store)
      (props, store)
    })
  }

  def getIndexId(label: Int, props: Array[Int]): Option[IndexId] = meta.getIndexId(label, props)

  def getIndexIdByLabel(label: Int): Set[IndexMeta] = meta.getIndexId(label)

  def allIndexId: Iterator[IndexMeta] = meta.all()

  def insertIndexRecord(indexId: IndexId, data: Any, id: Long, tx: LynxTransaction): Unit = {
    index.set(indexId, IndexEncoder.typeCode(data), IndexEncoder.encode(data), id, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexDB))
  }

  // todo: transaction
  def insertIndexRecordBatch(indexId: IndexId, data: Iterator[(Any, Long)], tx: LynxTransaction): Unit =
    index.set(indexId, data, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexDB))

  //data: (prop1Value, Prop2Value)
  def insertFulltextIndexRecord(indexId: IndexId, data: Array[Any], id: Long, tx: LynxTransaction): Unit = {
    val (propIds, store) = getFulltextIndex(indexId)
    store.insert(id, propIds.zip(data).toMap.map(p => {s"${p._1}" -> p._2.asInstanceOf[String]}))
    store.commit()
  }

  // todo: transaction
  def insertFulltextIndexRecordBatch(indexId: IndexId, data: Iterator[(Array[Any], Long)], tx: LynxTransaction): Unit = {
    val (propIds, store) = getFulltextIndex(indexId)
    data.foreach(d => {
      store.insert(d._2, propIds.zip(d._1).toMap.map(p => {s"${p._1}" -> p._2.asInstanceOf[String]}), maxBufferedDocs = 102400)
    })
    store.commit()
  }

  def updateIndexRecord(indexId: IndexId, value: Any, id: Long, newValue: Any, tx: LynxTransaction): Unit = {
    index.update(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value),
      id, IndexEncoder.typeCode(newValue), IndexEncoder.encode(newValue), tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexDB))
  }

  // todo: transaction
  def updateFulltextIndexRecord(indexId: IndexId, value: Any, id: Long, newValue: Array[Any], tx: LynxTransaction): Unit = {
    val (propIds, store) = getFulltextIndex(indexId)
    store.delete(id)
    store.insert(id, propIds.zip(newValue).toMap.map(p => {s"${p._1}" -> p._2.asInstanceOf[String]}))
  }

  def deleteIndexRecord(indexId: IndexId, value: Any, id: Long, tx: LynxTransaction): Unit ={
    index.delete(indexId, IndexEncoder.typeCode(value), IndexEncoder.encode(value), id, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexDB))
  }

  // todo: transaction
  def deleteFulltextIndexRecord(indexId: IndexId, value: Any, id: Long, tx: LynxTransaction): Unit =
    getFulltextIndex(indexId)._2.delete(id)

  def dropIndex(label: Int, props: Array[Int], tx: LynxTransaction): Unit = {
    meta.getIndexId(label, props).foreach{
      id=>
        index.deleteRange(id, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexDB))
        meta.deleteIndexMeta(label, props, false, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexMetaDB))
    }
  }

  // todo: transaction
  def dropFulltextIndex(label: Int, props: Array[Int], tx: LynxTransaction): Unit = {
    meta.getIndexId(label, props, true).foreach{
      id=>
        meta.deleteIndexMeta(label, props, true, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.indexMetaDB))
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
