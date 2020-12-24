package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.meta.{NodeLabelNameStore, PropertyNameStore, RelationTypeNameStore}
import cn.pandadb.kernel.store.{RelationStoreSPI, StoredRelation, StoredRelationWithProperty}

/**
 * @ClassName RelationStoreAPI
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/22
 * @Version 0.1
 */
class RelationStoreAPI(dbPath: String) extends RelationStoreSPI{

  private val relationDB = RocksDBStorage.getDB(s"${dbPath}/rels")
  private val relationStore = new RelationPropertyStore(relationDB)
  private val inRelationDB = RocksDBStorage.getDB(s"${dbPath}/inEdge")
  private val inRelationStore = new RelationDirectionStore(inRelationDB, RelationDirection.IN)
  private val outRelationDB = RocksDBStorage.getDB(s"${dbPath}/outEdge")
  private val outRelationStore = new RelationDirectionStore(outRelationDB, RelationDirection.OUT)
  private val relationLabelDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex")
  private val relationLabelStore = new RelationLabelIndex(relationLabelDB)
  private val metaDB = RocksDBStorage.getDB(s"${dbPath}/relationMeta")
  private val relationTypeNameStore = new RelationTypeNameStore(metaDB)
  private val propertyName = new PropertyNameStore(metaDB)

  override def allRelationTypes(): Array[String] = relationTypeNameStore.mapString2Int.keys.toArray

  override def allRelationTypeIds(): Array[Int] = relationTypeNameStore.mapInt2String.keys.toArray

  override def getRelationTypeName(relationTypeId: Int): String = relationTypeNameStore.key(relationTypeId).get

  override def getRelationTypeId(relationTypeName: String): Int = relationTypeNameStore.id(relationTypeName)

  override def addRelationType(relationTypeName: String): Int = relationTypeNameStore.id(relationTypeName)

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): String = propertyName.key(keyId).get

  override def getPropertyKeyId(keyName: String): Int = propertyName.id(keyName)

  override def addPropertyKey(keyName: String): Int = propertyName.id(keyName)

  override def getRelationById(relId: Long): StoredRelationWithProperty = relationStore.get(relId)

  override def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long] =
    relationLabelStore.getRelations(relationTypeId)

  override def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    val rel = relationStore.get(relationId)
    if (rel != null) {
      relationStore.set(new StoredRelationWithProperty(rel.id, rel.from, rel.to, rel.typeId,
        rel.properties ++ Map(propertyKeyId->propertyValue)))
    }
  }

  override def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any = {
    val rel = relationStore.get(relationId)
    if (rel != null) {
      relationStore.set(new StoredRelationWithProperty(rel.id, rel.from, rel.to, rel.typeId,
        rel.properties-propertyKeyId))
    }
  }

  override def findToNodeIds(fromNodeId: Long): Iterator[Long] = outRelationStore.getNodeIds(fromNodeId)

  override def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long] =
    outRelationStore.getNodeIds(fromNodeId, relationType)

  override def findFromNodeIds(toNodeId: Long): Iterator[Long] = inRelationStore.getNodeIds(toNodeId)

  override def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long] =
    inRelationStore.getNodeIds(toNodeId, relationType)

  override def addRelation(relation: StoredRelation): Unit = {
    relationStore.set(relation.id)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
  }

  override def addRelation(relation: StoredRelationWithProperty): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
  }

  override def deleteRelation(id: Long): Unit = {
    val relation = relationStore.get(id)
    relationStore.delete(id)
    inRelationStore.delete(relation)
    outRelationStore.delete(relation)
  }

  override def allRelationsWithProperty(): Iterator[StoredRelationWithProperty] = relationStore.all()

  override def allRelations(): Iterator[StoredRelation] = inRelationStore.all()

  override def findOutRelations(fromNodeId: Long): Iterator[StoredRelation] = outRelationStore.getRelations(fromNodeId)

  override def findOutRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelation] =
    outRelationStore.getRelations(fromNodeId, edgeType)

  override def findInRelations(toNodeId: Long): Iterator[StoredRelation] = inRelationStore.getRelations(toNodeId)

  override def findInRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelation] =
    inRelationStore.getRelations(toNodeId, edgeType)

  override def close(): Unit ={
    relationStore.close()
    inRelationStore.close()
    relationLabelStore.close()
  }
}
