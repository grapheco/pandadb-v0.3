package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}

/**
 * @ClassName RelationStoreAPI
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/22
 * @Version 0.1
 */
class RelationStoreAPI(dbPath: String) {

  private val relationDB = RocksDBStorage.getDB(s"${dbPath}/rels")
  private val relationStore = new RelationPropertyStore(relationDB)
  private val inRelationDB = RocksDBStorage.getDB(s"${dbPath}/inEdge")
  private val inRelationStore = new RelationDirectionStore(inRelationDB, RelationDirection.IN)
  private val outRelationDB = RocksDBStorage.getDB(s"${dbPath}/outEdge")
  private val outRelationStore = new RelationDirectionStore(outRelationDB, RelationDirection.OUT)
  private val relationLabelDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex")
  private val relationLabelStore = new RelationLabelIndex(relationLabelDB)


  def addRelation(relation: StoredRelation): Unit = {
    relationStore.set(relation.id)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
  }

  def addRelation(relation: StoredRelationWithProperty): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
  }

  def deleteRelation(id: Long): Unit = {
    val relation = relationStore.get(id)
    relationStore.delete(id)
    inRelationStore.delete(relation)
    outRelationStore.delete(relation)
  }

  def relationAt(id: Long): StoredRelationWithProperty = {
    relationStore.get(id)
  }

  def allRelationsWithProperty(): Iterator[StoredRelationWithProperty] = {
    relationStore.all()
  }

  def allRelations(): Iterator[StoredRelation] = {
    inRelationStore.all()
  }

  def getRelationsByType(typeId: Int): Iterator[Long] = {
    relationLabelStore.getRelations(typeId)
  }

  // out
  def findOutRelations(fromNodeId: Long): Iterator[StoredRelation] = {
    outRelationStore.getRelations(fromNodeId)
  }
  def findOutRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    outRelationStore.getRelations(fromNodeId, edgeType)
  }

  def findToNodes(fromNodeId: Long): Iterator[Long] = {
    outRelationStore.getNodeIds(fromNodeId)
  }
  def findToNodes(fromNodeId: Long, edgeType: Int): Iterator[Long] = {
    outRelationStore.getNodeIds(fromNodeId, edgeType)
  }

  // in
  def findInRelations(toNodeId: Long): Iterator[StoredRelation] = {
    inRelationStore.getRelations(toNodeId)
  }

  def findInRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    inRelationStore.getRelations(toNodeId, edgeType)
  }


  def findFromNodes(toNodeId: Long): Iterator[Long] = {
    inRelationStore.getNodeIds(toNodeId)
  }

  def findFromNodes(toNodeId: Long, edgeType: Int): Iterator[Long] = {
    inRelationStore.getNodeIds(toNodeId, edgeType)
  }



  def close(): Unit ={
    relationStore.close()
    inRelationStore.close()
    relationLabelStore.close()
  }


}
