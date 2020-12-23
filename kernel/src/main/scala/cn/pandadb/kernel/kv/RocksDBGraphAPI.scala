package cn.pandadb.kernel.kv

import java.io.File
import java.nio.file.Path

import cn.pandadb.kernel.kv.index.{IndexStore, IndexStoreAPI}
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.{RelationLabelIndex, RelationPropertyStore, RelationStoreAPI}
import cn.pandadb.kernel.{GraphRAM, NodeId, PropertyStore, TypedId}
import cn.pandadb.kernel.store.{MergedChanges, StoredNode, StoredNodeWithProperty, StoredNodeWithProperty_tobe_deprecated, StoredRelation, StoredRelationWithProperty}
import org.rocksdb.RocksDB
import sun.security.util.Length

class RocksDBGraphAPI(dbPath: String) {
  private val rocksDB = RocksDBStorage.getDB(s"${dbPath}/meta")

  private val nodeAPI = new NodeStoreAPI(dbPath)
  private val relationAPI = new RelationStoreAPI(dbPath)
  private val indexAPI = new IndexStoreAPI(dbPath)

  private val statInfoDB = RocksDBStorage.getDB(s"${dbPath}/statinfo")
  private val statInoStore = new StatInfo(statInfoDB)

  def getRocksDB: RocksDB = rocksDB

  def getMetaDB: RocksDB = rocksDB

  def clear(): Unit = {
  }

  def close(): Unit = {
    nodeAPI.close()
    relationAPI.close()
    indexAPI.close()
    rocksDB.close()
  }

  def addNode(node: StoredNodeWithProperty): Unit = nodeAPI.addNode(node)

  def deleteNode(id: Long): Unit = nodeAPI.deleteNode(id)

  def nodeAt(id: Long): StoredNodeWithProperty = nodeAPI.getNode(id)

  def findNodes(labelId: Int): Iterator[Long] = nodeAPI.getNodeIdsByLabel(labelId)

  // fixme 重复获取
  def allNodes(): Iterator[StoredNodeWithProperty] = nodeAPI.allNodes()

  def addRelation(relation: StoredRelation): Unit = relationAPI.addRelation(relation)

  def addRelation(relation: StoredRelationWithProperty): Unit = relationAPI.addRelation(relation)

  def deleteRelation(id: Long): Unit = relationAPI.deleteRelation(id)

  def relationAt(id: Long): StoredRelation = relationAPI.relationAt(id)

  def allRelations(): Iterator[StoredRelation] = relationAPI.allRelations()

  def getRelationsByType(typeId: Int): Iterator[Long] = relationAPI.getRelationsByType(typeId)

  def findOutRelations(fromNodeId: Long): Iterator[StoredRelation] = relationAPI.findOutRelations(fromNodeId)

  def findOutRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelation] =
    relationAPI.findOutRelations(fromNodeId, edgeType)

  def findToNodes(fromNodeId: Long): Iterator[Long] = relationAPI.findToNodes(fromNodeId)

  def findToNodes(fromNodeId: Long, edgeType: Int): Iterator[Long] = relationAPI.findToNodes(fromNodeId, edgeType)

  def findInRelations(toNodeId: Long): Iterator[StoredRelation] = relationAPI.findInRelations(toNodeId)

  def findInRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelation] =
    relationAPI.findInRelations(toNodeId, edgeType)

  def findFromNodes(toNodeId: Long): Iterator[Long] = relationAPI.findFromNodes(toNodeId)

  def findFromNodes(toNodeId: Long, edgeType: Int): Iterator[Long] = relationAPI.findFromNodes(toNodeId)

  // Index
  def createNodeIndex(nodeLabel: Int, nodePropertyIds: Array[Int]): Int =
    indexAPI.createIndex(nodeLabel, nodePropertyIds)

  def createNodeIndex(nodeLabel: Int, nodePropertyId: Int):  Int =
    indexAPI.createIndex(nodeLabel, Array(nodePropertyId))

  def getNodeIndexId(nodeLabel: Int, nodePropertyIds: Array[Int]):  Int =
    indexAPI.getIndexId(nodeLabel, nodePropertyIds)

  def dropNodeIndex(nodeLabel: Int, nodePropertyIds: Array[Int]): Unit =
    indexAPI.dropIndex(nodeLabel, nodePropertyIds)

  def insertNodeIndexRecord(indexId: Int, nodeId: Long, propertyValue: Any): Unit =
    indexAPI.insertIndexRecord(indexId, propertyValue, nodeId)

  def insertNodeIndexRecordsBatch(indexId: Int, data:Iterator[(Any, Long)]): Unit =
     indexAPI.insertIndexRecordBatch(indexId, data)

  def deleteNodeIndexRecord(indexId: Int, value: Any, nodeId: Int): Unit =
     indexAPI.deleteIndexRecord(indexId, value, nodeId)

  def updateNodeIndexRecord(indexId: Int, value: Any, nodeId: Int, newValue: Any): Unit =
     indexAPI.updateIndexRecord(indexId, value, nodeId, newValue)

  def findNodeIndexRecords(indexId: Int, value: Any): Iterator[Long] =
     indexAPI.find(indexId, value)

  def findIntRangeByIndex(indexId: Int, startValue: Int = Int.MinValue,
                          endValue: Int = Int.MaxValue): Iterator[Long] =
     indexAPI.findIntRange(indexId, startValue, endValue)

  def findStringStartWithByIndex(indexId: Int, startWith: String): Iterator[Long] =
     indexAPI.findStringStartWith(indexId, startWith)

  def findFloatRangeByIndex(indexId: Int, startValue: Float = Float.MinValue,
                            endValue: Float = Float.MaxValue): Iterator[Long] =
     indexAPI.findFloatRange(indexId, startValue, endValue)

  def getRelCountsByLabel(label: String): Option[Long] =
    statInoStore.getRelLabelCnt(label)

  def getCountsByLabel(label: String): Option[Long] = {
    statInoStore.getLabelCnt(label)
  }  //return counts ex: student->100, person-> 1000

  def getCountsByProperty(label: String, propertyName: String): Option[Long] = {
    statInoStore.getPropertyIndexCnt(label, propertyName)
  } //return counts ex: student with age :5000

  def getAllNodesCnt: Long ={
    statInoStore.getAllNodesCnt()
  }

  def getAllRelsCnt: Long ={
    statInoStore.getAllRelCnt()
  }
}