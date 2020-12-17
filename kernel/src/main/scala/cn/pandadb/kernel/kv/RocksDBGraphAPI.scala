package cn.pandadb.kernel.kv

import java.io.File
import java.nio.file.Path

import cn.pandadb.kernel.kv.NodeIndex.IndexId
import cn.pandadb.kernel.{GraphRAM, NodeId, PropertyStore, TypedId}
import cn.pandadb.kernel.store.{MergedChanges, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.rocksdb.RocksDB
import sun.security.util.Length

class RocksDBGraphAPI(dbPath: String) {
  private val rocksDB = RocksDBStorage.getDB(s"${dbPath}/meta")

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeStore = new NodeStore(nodeDB)
  private val relDB = RocksDBStorage.getDB(s"${dbPath}/rels")
  private val relationStore = new RelationStore(relDB)
  private val labelIndexDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabelIndex")
  private val nodeLabelIndex = new NodeLabelIndex(labelIndexDB)
  private val relIndexDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex")
  private val relationLabelIndex = new RelationLabelIndex(relIndexDB)
  private val inEdgeDB = RocksDBStorage.getDB(s"${dbPath}/inEdge")
  private val relationInEdgeIndex = new RelationInEdgeIndexStore(inEdgeDB)
  private val outEdgeDB = RocksDBStorage.getDB(s"${dbPath}/outEdge")
  private val relationOutEdgeIndex = new RelationOutEdgeIndexStore(outEdgeDB)
  private val nodeIndexDB = RocksDBStorage.getDB(s"${dbPath}/nodeIndex")
  private val nodeIndex = new NodeIndex(nodeIndexDB)

  def getRocksDB: RocksDB = rocksDB

  def clear(): Unit = {
  }

  def close(): Unit = {
    nodeDB.close()
    relDB.close()
    labelIndexDB.close()
    relIndexDB.close()
    inEdgeDB.close()
    outEdgeDB.close()
    nodeIndexDB.close()
    rocksDB.close()
  }

  // node operations
//  def addNode(t: StoredNode): Unit = {
//    nodeStore.set(t.id, t.labelIds, null)
//  }
//
//  def addNode(t: StoredNodeWithProperty): Unit = {
//    nodeStore.set(t.id, t.labelIds, t.properties)
//  }

  def addNode(nodeId: Long, labelIds: Array[Int], properties: Map[String, Any]): Unit = {
    nodeStore.set(nodeId, labelIds, properties)
    labelIds.foreach(labelId => nodeLabelIndex.add(labelId, nodeId))
  }

  def deleteNode(id: Long): Unit = {
    val node = nodeStore.delete(id)
    node.labelIds.foreach(labelId => nodeLabelIndex.delete(labelId, id))
  }

  def nodeAt(id: Long): StoredNodeWithProperty = {
    nodeStore.get(id)
  }

//  def nodes(): Iterator[StoredNode] = {
//    nodeStore.all()
//  }

  def findNodes(labelId: Int): Iterator[Long] = {
    nodeLabelIndex.getNodes(labelId)
  }

  def allNodes(): Iterator[StoredNodeWithProperty] = {
    nodeStore.all()
  }

  // relation operations
  def addRelation(t: StoredRelation): Unit = {
    relationStore.setRelation(t.id, t.from, t.to, t.labelId, 0, null)

    relationInEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, null)
    relationOutEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, null)
  }

  def addRelation(t: StoredRelationWithProperty): Unit = {
    relationStore.setRelation(t.id, t.from, t.to, t.labelId, t.category, t.properties)

    relationInEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, t.properties)
    relationOutEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, t.properties)
  }

  def addRelation(relId: Long, from: Long, to: Long, labelId: Int, propeties: Map[String, Any]): Unit = {
    relationStore.setRelation(relId, from, to, labelId, 0, propeties)

    relationInEdgeIndex.setIndex(from, labelId, 0, to, relId, propeties)
    relationOutEdgeIndex.setIndex(from, labelId, 0, to, relId, propeties)
  }

  def deleteRelation(id: Long): Unit = {
    val relation = relationStore.getRelation(id)
    relationStore.deleteRelation(id)

    relationInEdgeIndex.deleteIndex(relation.from, relation.labelId, relation.category, relation.to)
    relationOutEdgeIndex.deleteIndex(relation.from, relation.labelId, relation.category, relation.to)

  }
//
//  def deleteRelation(fromNode: Long, toNode: Long, labelId: Long, category: Long): Unit= {
//  }

  def relationAt(id: Long): StoredRelation = {
    relationStore.getRelation(id)
  }

  def allRelations(): Iterator[StoredRelation] = {
    relationStore.getAll()
  }

  def findRelations(labelId: Int): Iterator[Long] = {
    relationLabelIndex.getRelations(labelId)
  }
  // out
  def findOutEdgeRelations(fromNodeId: Long): Iterator[StoredRelation] = {
    relationOutEdgeIndex.getRelations(fromNodeId)
  }
  def findOutEdgeRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    relationOutEdgeIndex.getRelations(fromNodeId, edgeType)
  }
  def findOutEdgeRelations(fromNodeId: Long, edgeType: Int, category: Long): Iterator[StoredRelation] = {
    relationOutEdgeIndex.getRelations(fromNodeId, edgeType, category)
  }

  def findToNodes(fromNodeId: Long): Iterator[Long] = {
    relationOutEdgeIndex.findNodes(fromNodeId)
  }
  def findToNodes(fromNodeId: Long, edgeType: Int): Iterator[Long] = {
    relationOutEdgeIndex.findNodes(fromNodeId, edgeType)
  }
  def findToNodes(fromNodeId: Long, edgeType: Int, category: Long): Iterator[Long] = {
    relationOutEdgeIndex.findNodes(fromNodeId, edgeType, category)
  }

  // in
  def findInEdgeRelations(toNodeId: Long): Iterator[StoredRelation] = {
    relationInEdgeIndex.getRelations(toNodeId)
  }

  def findInEdgeRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    relationInEdgeIndex.getRelations(toNodeId, edgeType)
  }

  def findInEdgeRelations(toNodeId: Long, edgeType: Int, category: Long): Iterator[StoredRelation] = {
    relationInEdgeIndex.getRelations(toNodeId, edgeType, category)
  }

  def findFromNodes(toNodeId: Long): Iterator[Long] = {
    relationInEdgeIndex.findNodes(toNodeId)
  }

  def findFromNodes(toNodeId: Long, edgeType: Int): Iterator[Long] = {
    relationInEdgeIndex.findNodes(toNodeId, edgeType)
  }

  def findFromNodes(toNodeId: Long, edgeType: Int, category: Long): Iterator[Long] = {
    relationInEdgeIndex.findNodes(toNodeId, edgeType, category)
  }

  // Index
  def createNodeIndex(nodeLabel: Int, nodePropertyIds: Array[Int]): NodeIndex.IndexId = {
    nodeIndex.createIndex(nodeLabel, nodePropertyIds)
  }

  def createNodeIndex(nodeLabel: Int, nodePropertyId: Int): NodeIndex.IndexId = {
    nodeIndex.createIndex(nodeLabel, Array(nodePropertyId))
  }

  def getNodeIndexId(nodeLabel: Int, nodePropertyIds: Array[Int]): NodeIndex.IndexId = {
    nodeIndex.getIndexId(nodeLabel, nodePropertyIds)
  }

  def dropNodeIndex(nodeLabel: Int, nodePropertyIds: Array[Int]): Unit = {
    nodeIndex.dropIndex(nodeLabel, nodePropertyIds)
  }

  def insertNodeIndexRecord(indexId: IndexId, nodeId: Long, propertyValue: Any): Unit = {
    nodeIndex.insertIndexRecord(indexId, propertyValue, nodeId)
  }

  def insertNodeIndexRecordsBatch(indexId: IndexId, data:Iterator[(Any, Long)]): Unit = {
    nodeIndex.insertIndexRecordBatch(indexId, data)
  }

  def deleteNodeIndexRecord(indexId: IndexId, value: Any, nodeId: Int): Unit = {
    nodeIndex.deleteIndexRecord(indexId, value, nodeId)
  }

  def updateNodeIndexRecord(indexId: IndexId, value: Any, nodeId: Int, newValue: Any): Unit = {
    nodeIndex.updateIndexRecord(indexId, value, nodeId, newValue)
  }

  def findNodeIndexRecords(indexId: IndexId, value: Any): Iterator[Long] = {
    nodeIndex.find(indexId, value)
  }

  def findIntRangeByIndex(indexId: IndexId, startValue: Int = Int.MinValue, endValue: Int = Int.MaxValue): Iterator[Long] = {
    nodeIndex.findIntRange(indexId, startValue, endValue)
  }

  def findStringStartWithByIndex(indexId: IndexId, startWith: String): Iterator[Long] = {
    nodeIndex.findStringStartWith(indexId, startWith)
  }

  def findFloatRangeByIndex(indexId: IndexId, startValue: Float = Float.MinValue, endValue: Float = Float.MaxValue): Iterator[Long] = {
    nodeIndex.findFloatRange(indexId, startValue, endValue)
  }

  def getCountsByLabel(label: String): Int = ???  //return counts ex: student->100, person-> 1000
  def getCountsByLabel(labels: Set[String]): Array[(String, Int)] = ???  //return counts ex: student->100, person-> 1000
  def getCountsByProperty(labels: Set[String], propertyName: String): Int = ??? //return counts ex: student with age :5000


//  // below is the code added by zhaozihao, for possible further use.
//  def relsFrom(id: Long): Iterable[StoredRelation] = ???
//  def relsTo(id: Long): Iterable[StoredRelation] = ???
//
//  def searchByLabel(label: Label): Iterable[StoredNode] = ???
//  def searchByType(t: Type): Iterable[StoredRelation] = ???
//
//  //essential? could check whether index available in implemantion.
//  def searchByIndexedProperty(stat: Stat): Iterable[StoredNode] = ???
//  def searchByCategory(category: Category): Iterable[StoredRelation] = ???
//
//  def searchByProp(stat: Stat): Iterable[StoredNode] = ???





}


//case class Label(label: String)
//case class Type(t: String)
//
//case class Stat()
//case class Category()