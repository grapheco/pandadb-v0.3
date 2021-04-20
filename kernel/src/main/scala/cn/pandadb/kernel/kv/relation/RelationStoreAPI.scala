package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.meta.{IdGenerator, NodeLabelNameStore, PropertyNameStore, RelationTypeNameStore}
import cn.pandadb.kernel.store.{RelationStoreSPI, StoredRelation, StoredRelationWithProperty}

/**
 * @ClassName RelationStoreAPI
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/22
 * @Version 0.1
 */
class RelationStoreAPI(dbPath: String, rocksdbCfgPath: String = "default") extends RelationStoreSPI{

  private val relationDB = RocksDBStorage.getDB(s"${dbPath}/rels", rocksdbConfigPath = rocksdbCfgPath)
  private val relationStore = new RelationPropertyStore(relationDB)
  private val inRelationDB = RocksDBStorage.getDB(s"${dbPath}/inEdge", rocksdbConfigPath = rocksdbCfgPath)
  private val inRelationStore = new RelationDirectionStore(inRelationDB, RelationDirection.IN)
  private val outRelationDB = RocksDBStorage.getDB(s"${dbPath}/outEdge", rocksdbConfigPath = rocksdbCfgPath)
  private val outRelationStore = new RelationDirectionStore(outRelationDB, RelationDirection.OUT)
  private val relationLabelDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex", rocksdbConfigPath = rocksdbCfgPath)
  private val relationLabelStore = new RelationLabelIndex(relationLabelDB)
  private val metaDB = RocksDBStorage.getDB(s"${dbPath}/relationMeta", rocksdbConfigPath = rocksdbCfgPath)
  private val relationTypeNameStore = new RelationTypeNameStore(metaDB)
  private val propertyName = new PropertyNameStore(metaDB)
  private val relationIdGenerator = new IdGenerator(relationDB, 200)

  override def allRelationTypes(): Array[String] = relationTypeNameStore.mapString2Int.keys.toArray

  override def allRelationTypeIds(): Array[Int] = relationTypeNameStore.mapInt2String.keys.toArray

  override def relationCount: Long = relationStore.count

  override def getRelationTypeName(relationTypeId: Int): Option[String] = relationTypeNameStore.key(relationTypeId)

  override def getRelationTypeId(relationTypeName: String): Int = relationTypeNameStore.id(relationTypeName)

  override def addRelationType(relationTypeName: String): Int = relationTypeNameStore.id(relationTypeName)

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Int = propertyName.id(keyName)

  override def addPropertyKey(keyName: String): Int = propertyName.id(keyName)

  override def getRelationById(relId: Long): Option[StoredRelationWithProperty] = relationStore.get(relId)

  override def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long] =
    relationLabelStore.getRelations(relationTypeId)

  override def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit = {
    relationStore.get(relationId).foreach{
      rel =>
      relationStore.set(new StoredRelationWithProperty(rel.id, rel.from, rel.to, rel.typeId,
        rel.properties ++ Map(propertyKeyId->propertyValue)))
    }
  }

  override def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any = {
    relationStore.get(relationId).foreach{
      rel =>
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
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
    relationLabelStore.set(relation.typeId, relation.id)
  }

  override def addRelation(relation: StoredRelationWithProperty): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
    relationLabelStore.set(relation.typeId, relation.id)
  }

  override def deleteRelation(id: Long): Unit = {
    relationStore.get(id).foreach{
      relation =>
        relationStore.delete(id)
        inRelationStore.delete(relation)
        outRelationStore.delete(relation)
        relationLabelStore.delete(relation.typeId, relation.id)
    }
  }

  override def allRelations(withProperty: Boolean=false): Iterator[StoredRelation] = {
    if(withProperty) relationStore.all()
    else inRelationStore.all()
  }

  override def findOutRelations(fromNodeId: Long, edgeType:Option[Int] = None): Iterator[StoredRelation] =
    edgeType.map(outRelationStore.getRelations(fromNodeId, _))
      .getOrElse(outRelationStore.getRelations(fromNodeId))

  override def findInRelations(toNodeId: Long, edgeType:Option[Int] = None): Iterator[StoredRelation] =
    edgeType.map(inRelationStore.getRelations(toNodeId, _))
      .getOrElse(inRelationStore.getRelations(toNodeId))

  override def findInRelationsBetween(toNodeId: Long, fromNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation] = {
    edgeType.map(inRelationStore.getRelation(toNodeId, _, fromNodeId).toIterator)
      .getOrElse(inRelationStore.getRelations(toNodeId).filter(r => r.from == fromNodeId))
  }

  override def findOutRelationsBetween(fromNodeId: Long, toNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation] = {
    edgeType.map(outRelationStore.getRelation(fromNodeId, _, toNodeId).toIterator)
      .getOrElse(outRelationStore.getRelations(fromNodeId).filter(r => r.to == toNodeId))
  }


  override def close(): Unit ={
//    relationIdGenerator.flush()
    relationStore.close()
    inRelationStore.close()
    outRelationDB.close()
    metaDB.close()
    relationLabelStore.close()
  }

  override def newRelationId(): Long = {
    relationIdGenerator.nextId()
  }
}
