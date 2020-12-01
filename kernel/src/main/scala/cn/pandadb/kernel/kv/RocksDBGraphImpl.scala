package cn.pandadb.kernel.kv

import cn.pandadb.kernel.{GraphRAM, NodeId, PropertyStore, TypedId}
import cn.pandadb.kernel.store.{MergedChanges, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}

class RocksDBGraphImpl(dbPath: String) {
  private val rocksDB = RocksDBStorage.getDB(dbPath)
  private val nodeStore = new NodeStore(rocksDB)
  private val relationStore = new RelationStore(rocksDB)
  private val nodeLabelIndex = new NodeLabelIndex(rocksDB)

  def clear(): Unit = {
  }

  def close(): Unit = {
    rocksDB.close()
  }

  // node operations
  def addNode(t: StoredNode): Unit = {
    nodeStore.set(t.id, t.labelIds, null)
  }

  def addNode(t: StoredNodeWithProperty): Unit = {
    nodeStore.set(t.id, t.labelIds, t.properties)
  }

  def addNode(nodeId: Long, labelIds: Array[Int], propeties: Map[String, Any]): Unit = {
    nodeStore.set(nodeId, labelIds, propeties)
  }

  def deleteNode(id: Long): Unit = {
    nodeStore.delete(id)
  }

  def nodeAt(id: Long): StoredNode = {
    nodeStore.get(id)
  }

  def nodes(): Iterator[StoredNode] = {
    nodeStore.all()
  }

  def nodes(labelId: Int): Iterator[Long] = {
    nodeLabelIndex.getNodes(labelId)
  }

  def allNodes(): Iterator[StoredNode] = {
    nodeStore.all()
  }

  // relation operations
  def addRelation(t: StoredRelation): Unit = {
    relationStore.setRelation(t.id, t.from, t.to, t.labelId, 0, null)
  }

  def addRelation(t: StoredRelationWithProperty): Unit = {
    relationStore.setRelation(t.id, t.from, t.to, t.labelId, t.category, t.properties)
  }

  def addRelation(relId: Long, from: Long, to: Long, labelId: Int, propeties: Map[String, Any]): Unit = {
    relationStore.setRelation(relId, from, to, labelId, 0, propeties)
  }

  def deleteRelation(id: Long): Unit = {
    relationStore.deleteRelation(id)
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

  def relationScanByLabel(labelId: Int): Iterator[StoredRelation] = {
    null
  }


  // below is the code added by zhaozihao, for possible further use.
  def relsFrom(id: Long): Iterable[StoredRelation] = ???
  def relsTo(id: Long): Iterable[StoredRelation] = ???

  def searchByLabel(label: Label): Iterable[StoredNode] = ???
  def searchByType(t: Type): Iterable[StoredRelation] = ???

  //essential? could check whether index available in implemantion.
  def searchByIndexedProperty(stat: Stat): Iterable[StoredNode] = ???
  def searchByCategory(category: Category): Iterable[StoredRelation] = ???

  def searchByProp(stat: Stat): Iterable[StoredNode] = ???





}


case class Label(label: String)
case class Type(t: String)

case class Stat()
case class Category()