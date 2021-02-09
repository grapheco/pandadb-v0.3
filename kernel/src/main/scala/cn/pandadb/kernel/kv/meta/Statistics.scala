package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.db.KeyValueDB
import cn.pandadb.kernel.kv.meta.Statistics.{INDEXPROPERTYCOUNT, NODELABELCOUNT, NODESCOUNT, RELATIONSCOUNT, RELATIONTYPECOUNT, emptyLong}
import cn.pandadb.kernel.kv.{ByteUtils, RocksDBStorage}

import scala.collection.mutable


object Statistics {
  val NODESCOUNT: Byte = 1
  val RELATIONSCOUNT: Byte = 2
  val NODELABELCOUNT: Byte = 3
  val RELATIONTYPECOUNT: Byte =  4
  val INDEXPROPERTYCOUNT: Byte = 5

  val emptyLong: Array[Byte] = ByteUtils.longToBytes(0)
}

class Statistics(path: String, rocksdbCfgPath: String = "default") {

  val db: KeyValueDB = RocksDBStorage.getDB(s"${path}/statistics", rocksdbConfigPath = rocksdbCfgPath)

  var allNodesCount: Long = -1
  var allRelationCount: Long = -1
  var nodeLabelCount: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  var relationTypeCount: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  var indexPropertyCount: mutable.Map[Int, Long] = mutable.Map[Int, Long]()

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
    allNodesCount = ByteUtils.getLong(getValue(Array(NODESCOUNT)).getOrElse(emptyLong), 0)
    allRelationCount = ByteUtils.getLong(getValue(Array(RELATIONSCOUNT)).getOrElse(emptyLong), 0)
    nodeLabelCount.clear()
    relationTypeCount.clear()
    indexPropertyCount.clear()
    nodeLabelCount = getMap(Array(NODELABELCOUNT))
    relationTypeCount = getMap(Array(NODELABELCOUNT))
    indexPropertyCount = getMap(Array(NODELABELCOUNT))
  }

  def flush(): Unit = {
    db.put(Array(NODESCOUNT), ByteUtils.longToBytes(allNodesCount))
    db.put(Array(RELATIONSCOUNT), ByteUtils.longToBytes(allRelationCount))
    nodeLabelCount.foreach{
      kv=>
      db.put(getKey(NODELABELCOUNT, kv._1), ByteUtils.longToBytes(kv._2))
    }
    relationTypeCount.foreach{
      kv=>
      db.put(getKey(RELATIONTYPECOUNT, kv._1), ByteUtils.longToBytes(kv._2))
    }
    indexPropertyCount.foreach{
      kv=>
      db.put(getKey(INDEXPROPERTYCOUNT, kv._1), ByteUtils.longToBytes(kv._2))
    }
    db.flush()
  }

  def nodeCount: Long = allNodesCount

  def nodeCount_=(count: Long): Unit = allNodesCount = count

  def increaseNodeCount(count: Long): Unit = allNodesCount += count

  def decreaseNodes(count: Long): Unit = allNodesCount -= count

  def relationCount: Long = allRelationCount

  def relationCount_=(count: Long): Unit = allRelationCount = count

  def increaseRelationCount(count: Long): Unit = allRelationCount += count

  def decreaseRelations(count: Long): Unit = allRelationCount -= count

  def getNodeLabelCount(labelId: Int): Option[Long] = nodeLabelCount.get(labelId)

  def setNodeLabelCount(labelId: Int, count: Long): Unit =
    nodeLabelCount += labelId -> count

  def increaseNodeLabelCount(labelId: Int, count: Long): Unit =
    nodeLabelCount += labelId -> (nodeLabelCount.getOrElse(labelId, 0L) + count)

  def decreaseNodeLabelCount(labelId: Int, count: Long): Unit =
    nodeLabelCount += labelId -> (nodeLabelCount.getOrElse(labelId, 0L) - count)

  def getRelationTypeCount(typeId: Int): Option[Long] = relationTypeCount.get(typeId)

  def setRelationTypeCount(typeId: Int, count: Long): Unit =
    relationTypeCount += typeId -> count

  def increaseRelationTypeCount(typeId: Int, count: Long): Unit =
    relationTypeCount += typeId -> (relationTypeCount.getOrElse(typeId, 0L) + count)

  def decreaseRelationLabelCount(typeId: Int, count: Long): Unit =
    relationTypeCount += typeId -> (relationTypeCount.getOrElse(typeId, 0L) - count)

  def getIndexPropertyCount(indexId: Int): Option[Long] = indexPropertyCount.get(indexId)

  def setIndexPropertyCount(indexId: Int, count: Long): Unit =
    indexPropertyCount += indexId -> count

  def increaseIndexPropertyCount(indexId: Int, count: Long): Unit =
    indexPropertyCount += indexId -> (indexPropertyCount.getOrElse(indexId, 0L) + count)

  def decreaseIndexPropertyCount(indexId: Int, count: Long): Unit =
    indexPropertyCount += indexId -> (indexPropertyCount.getOrElse(indexId, 0L) - count)

  def close(): Unit = db.close()
}
