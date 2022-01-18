package org.grapheco.pandadb.kernel.distribute.meta

import java.util.concurrent.atomic.AtomicLong

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.kv.ByteUtils

import scala.collection.mutable

/**
 * @program: pandadb-v0.3
 * @description: todo: atomic
 *               prop store: divided by label
 *               example: n1: ["worker", "coder"]("name": "A"),
 *               if "name" indexed, then 2 propCount added ("worker"->"name", "coder"->"name)
 * @author: LiamGao
 * @create: 2021-12-19 20:27
 */
class DistributedStatistics(db: DistributedKVAPI) {
  private val NODES_COUNT: Byte = 1
  private val RELATIONS_COUNT: Byte = 2
  private val NODE_COUNT_BY_LABEL: Byte = 3
  private val RELATION_COUNT_BY_TYPE: Byte = 4
  private val PROPERTY_COUNT_BY_INDEX: Byte = 5

  var _allNodesCount: AtomicLong = new AtomicLong(0)
  var _allRelationCount: AtomicLong = new AtomicLong(0)
  var _nodeCountByLabel: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  var _relationCountByType: mutable.Map[Int, Long] = mutable.Map[Int, Long]()
  var _propertyCountByIndex: mutable.Map[Int, Long] = mutable.Map[Int, Long]()

  def getNodeLabelCountMap = _nodeCountByLabel.toMap

  def getRelationTypeCountMap = _relationCountByType.toMap

  def getPropertyCountByIndex = _propertyCountByIndex.toMap

  private def getKey(prefix: Byte, key: Int): Array[Byte] = {
    val res = new Array[Byte](6)
    ByteUtils.setByte(res, 0, DistributedKeyConverter.statisticPrefix)
    ByteUtils.setByte(res, 1, prefix)
    ByteUtils.setInt(res, 2, key)
    res
  }

  private def getValue(key: Array[Byte]): Array[Byte] = {
    db.get(Array(DistributedKeyConverter.statisticPrefix) ++ key)
  }

  private def getMap(prefix: Array[Byte]): mutable.Map[Int, Long] = {
    val res = mutable.Map[Int, Long]()
    val iter = db.scanPrefix(Array(DistributedKeyConverter.statisticPrefix) ++ prefix, 1000, false)
    while (iter.hasNext) {
      val kv = iter.next()
      res += ByteUtils.getInt(kv.getKey.toByteArray, prefix.length + 1) -> ByteUtils.getLong(kv.getValue.toByteArray, 0)
    }
    res
  }

  def init(): Unit = {
    val nc = {
      val value = getValue(Array(NODES_COUNT))
      if (value.nonEmpty) ByteUtils.getLong(value, 0)
      else 0
    }
    _allNodesCount.set(nc)

    val arc = {
      val value = getValue(Array(RELATIONS_COUNT))
      if (value.nonEmpty) ByteUtils.getLong(value, 0)
      else 0
    }
    _allRelationCount.set(arc)

//    _nodeCountByLabel.clear()
//    _relationCountByType.clear()
//    _propertyCountByIndex.clear()
    _nodeCountByLabel = getMap(Array(NODE_COUNT_BY_LABEL))
    _relationCountByType = getMap(Array(RELATION_COUNT_BY_TYPE))
    _propertyCountByIndex = getMap(Array(PROPERTY_COUNT_BY_INDEX))
  }

  def refreshMeta(): Unit = {
    val anc = {
      val value = getValue(Array(NODES_COUNT))
      if (value.nonEmpty) ByteUtils.getLong(value, 0)
      else 0
    }
    val arc = {
      val value = getValue(Array(RELATIONS_COUNT))
      if (value.nonEmpty) ByteUtils.getLong(value, 0)
      else 0
    }
    val ncbl = getMap(Array(NODE_COUNT_BY_LABEL))
    val rcbt = getMap(Array(RELATION_COUNT_BY_TYPE))
    val pcbi = getMap(Array(PROPERTY_COUNT_BY_INDEX))

    _allNodesCount.set(anc)
    _allRelationCount.set(arc)
    _nodeCountByLabel = ncbl
    _relationCountByType = rcbt
    _propertyCountByIndex = pcbi
  }

  def clean(): Unit = {
    _allNodesCount.set(0)
    _allRelationCount.set(0)
    _nodeCountByLabel = mutable.Map[Int, Long]()
    _relationCountByType = mutable.Map[Int, Long]()
    _propertyCountByIndex = mutable.Map[Int, Long]()
  }

  def flush(): Unit = {
    db.put(Array(DistributedKeyConverter.statisticPrefix) ++ Array(NODES_COUNT), ByteUtils.longToBytes(_allNodesCount.get()))
    db.put(Array(DistributedKeyConverter.statisticPrefix) ++ Array(RELATIONS_COUNT), ByteUtils.longToBytes(_allRelationCount.get()))
    _nodeCountByLabel.foreach {
      kv =>
        db.put(getKey(NODE_COUNT_BY_LABEL, kv._1), ByteUtils.longToBytes(kv._2))
    }
    _relationCountByType.foreach {
      kv =>
        db.put(getKey(RELATION_COUNT_BY_TYPE, kv._1), ByteUtils.longToBytes(kv._2))
    }
    _propertyCountByIndex.foreach {
      kv =>
        db.put(getKey(PROPERTY_COUNT_BY_INDEX, kv._1), ByteUtils.longToBytes(kv._2))
    }
  }

  def nodeCount: Long = _allNodesCount.get()

  def setNodeCount(count: Long): Unit = _allNodesCount.set(count)

  def increaseNodeCount(count: Long): Unit = _allNodesCount.addAndGet(count)

  def decreaseNodes(count: Long): Unit = {
    _allNodesCount.set(_allNodesCount.get() - count)
    if (_allNodesCount.get() <= 0) _allNodesCount.set(0)
  }

  def relationCount: Long = _allRelationCount.get()

  def setRelationCount(count: Long): Unit = _allRelationCount.set(count)

  def increaseRelationCount(count: Long): Unit = _allRelationCount.addAndGet(count)

  def decreaseRelations(count: Long): Unit = {
    _allRelationCount.set(_allRelationCount.get() - count)
    if (_allRelationCount.get() <= 0) _allRelationCount.set(0)
  }

  def getNodeLabelCount(labelId: Int): Option[Long] = _nodeCountByLabel.get(labelId)

  def setNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> count

  def increaseNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> (_nodeCountByLabel.getOrElse(labelId, 0L) + count)

  def decreaseNodeLabelCount(labelId: Int, count: Long): Unit =
    _nodeCountByLabel += labelId -> math.max(_nodeCountByLabel.getOrElse(labelId, 0L) - count, 0)

  def getRelationTypeCount(typeId: Int): Option[Long] = _relationCountByType.get(typeId)

  def setRelationTypeCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> count

  def increaseRelationTypeCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> (_relationCountByType.getOrElse(typeId, 0L) + count)

  def decreaseRelationLabelCount(typeId: Int, count: Long): Unit =
    _relationCountByType += typeId -> math.max(_relationCountByType.getOrElse(typeId, 0L) - count, 0)

  def getIndexPropertyCount(indexedPropId: Int): Option[Long] = _propertyCountByIndex.get(indexedPropId)

  def setIndexPropertyCount(indexedPropId: Int, count: Long): Unit =
    _propertyCountByIndex += indexedPropId -> count

  def increaseIndexPropertyCount(indexedPropId: Int, count: Long): Unit =
    _propertyCountByIndex += indexedPropId -> (_propertyCountByIndex.getOrElse(indexedPropId, 0L) + count)

  def decreaseIndexPropertyCount(indexedPropId: Int, count: Long): Unit =
    _propertyCountByIndex += indexedPropId -> math.max(_propertyCountByIndex.getOrElse(indexedPropId, 0L) - count, 0)

}
