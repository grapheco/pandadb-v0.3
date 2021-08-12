package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.meta.Statistics._
import cn.pandadb.kernel.kv.{ByteUtils, RocksDBStorage, TransactionRocksDBStorage}
import cn.pandadb.kernel.transaction.DBNameMap
import org.rocksdb.{Transaction, TransactionDB, WriteOptions}

import scala.collection.mutable


class TransactionStatistics(db: TransactionDB) {

  private var _allNodesCount: Long = -1
  private var _allRelationCount: Long = -1
  private var _nodeCountByLabel: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  private var _relationCountByType: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  private var _propertyCountByIndex: mutable.Map[Int, Long] = mutable.Map[Int, Long]()

  def generateTransactions(writeOptions: WriteOptions): Map[String, Transaction] = {
    Map(DBNameMap.statisticsDB -> db.beginTransaction(writeOptions))
  }

  private def getKey(prefix: Byte, key: Int): Array[Byte] = {
    val res = new Array[Byte](5)
    ByteUtils.setByte(res, 0, prefix)
    ByteUtils.setInt(res, 1, key)
    res
  }

  private def getValue(key: Array[Byte]): Option[Array[Byte]] = {
    Option(db.get(key))
  }

  private def getMap(prefix: Array[Byte]): mutable.Map[Int, Long] = {
    val res = mutable.Map[Int, Long]()
    val iter = db.newIterator()
    iter.seek(prefix)
    while (iter.isValid && iter.key().startsWith(prefix)){
      res += ByteUtils.getInt(iter.key(), prefix.length) -> ByteUtils.getLong(iter.value(), 0)
      iter.next()
    }
    res
  }

  def init(): Unit = {
    _allNodesCount = ByteUtils.getLong(getValue(Array(NODESCOUNT)).getOrElse(emptyLong), 0)
    _allRelationCount = ByteUtils.getLong(getValue(Array(RELATIONSCOUNT)).getOrElse(emptyLong), 0)
    _nodeCountByLabel.clear()
    _relationCountByType.clear()
    _propertyCountByIndex.clear()
    _nodeCountByLabel = getMap(Array(NODECOUNTBYLABEL))
    _relationCountByType = getMap(Array(NODECOUNTBYLABEL))
    _propertyCountByIndex = getMap(Array(NODECOUNTBYLABEL))
  }

  def flush(tx: Transaction): Unit = {
    tx.put(Array(NODESCOUNT), ByteUtils.longToBytes(_allNodesCount))
    tx.put(Array(RELATIONSCOUNT), ByteUtils.longToBytes(_allRelationCount))
    _nodeCountByLabel.foreach{
      kv=>
        tx.put(getKey(NODECOUNTBYLABEL, kv._1), ByteUtils.longToBytes(kv._2))
    }
    _relationCountByType.foreach{
      kv=>
        tx.put(getKey(RELATIONCOUNTBYTYPE, kv._1), ByteUtils.longToBytes(kv._2))
    }
    _propertyCountByIndex.foreach{
      kv=>
        tx.put(getKey(PROPERTYCOUNTBYINDEX, kv._1), ByteUtils.longToBytes(kv._2))
    }
  }

  def nodeCount: Long = _allNodesCount

  def nodeCount_=(count: Long): Unit = _allNodesCount = count

  def increaseNodeCount(count: Long): Unit = _allNodesCount += count

  def decreaseNodes(count: Long): Unit = _allNodesCount -= count

  def relationCount: Long = _allRelationCount

  def relationCount_=(count: Long): Unit = _allRelationCount = count

  def increaseRelationCount(count: Long): Unit = _allRelationCount += count

  def decreaseRelations(count: Long): Unit = _allRelationCount -= count

  def getNodeLabelCount(labelId: Int): Option[Long] = _nodeCountByLabel.get(labelId)

  def setNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> count

  def increaseNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> (_nodeCountByLabel.getOrElse(labelId, 0L) + count)

  def decreaseNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> (_nodeCountByLabel.getOrElse(labelId, 0L) - count)

  def getRelationTypeCount(typeId: Int): Option[Long] = _relationCountByType.get(typeId)

  def setRelationTypeCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> count

  def increaseRelationTypeCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> (_relationCountByType.getOrElse(typeId, 0L) + count)

  def decreaseRelationLabelCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> (_relationCountByType.getOrElse(typeId, 0L) - count)

  def getIndexPropertyCount(indexId: Int): Option[Long] = _propertyCountByIndex.get(indexId)

  def setIndexPropertyCount(indexId: Int, count: Long): Unit =
    _propertyCountByIndex += indexId -> count

  def increaseIndexPropertyCount(indexId: Int, count: Long): Unit =
    _propertyCountByIndex += indexId -> (_propertyCountByIndex.getOrElse(indexId, 0L) + count)

  def decreaseIndexPropertyCount(indexId: Int, count: Long): Unit =
    _propertyCountByIndex += indexId -> (_propertyCountByIndex.getOrElse(indexId, 0L) - count)

  def close(): Unit = db.close()
}
