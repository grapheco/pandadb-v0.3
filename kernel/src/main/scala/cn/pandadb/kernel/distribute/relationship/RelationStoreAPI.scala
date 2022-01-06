package cn.pandadb.kernel.distribute.relationship

import cn.pandadb.kernel.distribute.DistributedKVAPI
import cn.pandadb.kernel.distribute.meta.{IdGenerator, PropertyNameStore, RelationTypeNameStore, TypeNameEnum}
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-18 10:53
 */
class RelationStoreAPI(db: DistributedKVAPI, propertyNameStore: PropertyNameStore) extends DistributedRelationStoreSPI {
  private val relationTypeNameStore = new RelationTypeNameStore(db, propertyNameStore.udpClients)
  private val idGenerator =new IdGenerator(db, TypeNameEnum.relationName)

  val inRelationStore = new RelationDirectionStore(db, RelationDirection.IN)
  val outRelationStore = new RelationDirectionStore(db, RelationDirection.OUT)
  val relationTypeStore = new RelationTypeStore(db)
  val relationStore = new RelationPropertyStore(db)

  override def refreshMeta(): Unit = {
    relationTypeNameStore.refreshNameStore()
    propertyNameStore.refreshNameStore()
    idGenerator.refreshId()
  }

  override def newRelationId(): Long = idGenerator.nextId()

  override def cleanData(): Unit = idGenerator.resetId()

  override def getRelationTypeName(relationTypeId: Int): Option[String] = relationTypeNameStore.key(relationTypeId)

  override def getRelationTypeId(relationTypeName: String): Option[Int] = relationTypeNameStore.id(relationTypeName)

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyNameStore.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyNameStore.id(keyName)

  override def getRelationById(relId: Long): Option[StoredRelationWithProperty] = relationStore.get(relId)

  override def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long] = relationTypeStore.getRelationIds(relationTypeId)

  override def addRelation(relation: StoredRelation): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
    relationTypeStore.set(relation.typeId, relation.id)
  }

  override def addRelation(relation: StoredRelationWithProperty): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
    relationTypeStore.set(relation.typeId, relation.id)
    idGenerator.flushId()
  }

  override def addRelationType(relationTypeName: String): Int = {
    relationTypeNameStore.getOrAddId(relationTypeName)
  }

  override def addPropertyKey(keyName: String): Int = {
    propertyNameStore.getOrAddId(keyName)
  }

  override def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    relationStore.get(relationId).foreach(
      rel => {
        relationStore.set(
          new StoredRelationWithProperty(
            rel.id, rel.from, rel.to, rel.typeId, rel.properties ++ Map(propertyKeyId -> propertyValue))
        )
      }
    )
  }

  override def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any = {
    relationStore.get(relationId).foreach(
      rel => {
        relationStore.set(
          new StoredRelationWithProperty(
            rel.id, rel.from, rel.to, rel.typeId, rel.properties - propertyKeyId)
        )
      }
    )
  }

  override def deleteRelation(relationId: Long): Unit = {
    relationStore.get(relationId).foreach(
      relation => {
        relationStore.delete(relationId)
        inRelationStore.delete(relation)
        outRelationStore.delete(relation)
        relationTypeStore.delete(relation.typeId, relationId)
      }
    )
  }

  override def findToNodeIds(fromNodeId: Long): Iterator[Long] = outRelationStore.getRightNodeIds(fromNodeId)

  override def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long] = outRelationStore.getRightNodeIds(fromNodeId, relationType)

  override def findFromNodeIds(toNodeId: Long): Iterator[Long] = inRelationStore.getRightNodeIds(toNodeId)

  override def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long] = inRelationStore.getRightNodeIds(toNodeId, relationType)

  override def findOutRelations(fromNodeId: Long, edgeType: Option[Int]): Iterator[StoredRelation] = {
    edgeType.map(outRelationStore.getRelations(fromNodeId, _))
      .getOrElse(outRelationStore.getRelations(fromNodeId))
  }

  override def findInRelations(toNodeId: Long, edgeType: Option[Int]): Iterator[StoredRelation] = {
    edgeType.map(inRelationStore.getRelations(toNodeId, _))
      .getOrElse(inRelationStore.getRelations(toNodeId))
  }

  override def allRelationTypes(): Array[String] = relationTypeNameStore.mapString2Int.keys.toArray

  override def allRelationTypeIds(): Array[Int] = relationTypeNameStore.mapInt2String.keys.toArray

  override def allPropertyKeys(): Array[String] = propertyNameStore.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyNameStore.mapInt2String.keys.toArray

  override def allRelations(): Iterator[StoredRelation] = relationStore.all()

  override def relationCount: Long = relationStore.count

  override def close(): Unit = {idGenerator.flushId()}
}

trait DistributedRelationStoreSPI {
  def refreshMeta(): Unit

  def newRelationId(): Long;

  def cleanData(): Unit

  def getRelationTypeName(relationTypeId: Int): Option[String];

  def getRelationTypeId(relationTypeName: String): Option[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def getRelationById(relId: Long): Option[StoredRelationWithProperty];

  def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long];

  def addRelation(relation: StoredRelation): Unit

  def addRelation(relation: StoredRelationWithProperty): Unit

  def addRelationType(relationTypeName: String): Int;

  def addPropertyKey(keyName: String): Int;

  def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any;

  def deleteRelation(relationId: Long): Unit;

  def findToNodeIds(fromNodeId: Long): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];

  def findFromNodeIds(toNodeId: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];

  def findOutRelations(fromNodeId: Long): Iterator[StoredRelation] = findOutRelations(fromNodeId, None)

  def findOutRelations(fromNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def findInRelations(toNodeId: Long): Iterator[StoredRelation] = findInRelations(toNodeId, None)

  def findInRelations(toNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def allRelationTypes(): Array[String];

  def allRelationTypeIds(): Array[Int];

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def allRelations(): Iterator[StoredRelation]

  def relationCount: Long

  def close(): Unit
}