package cn.pandadb.kernel.kv.relation

import cn.pandadb.kernel.kv.meta.{IdGenerator, PropertyNameStore, RelationTypeNameStore, TransactionIdGenerator, TransactionPropertyNameStore, TransactionRelationTypeNameStore}
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty, TransactionRelationStoreSPI}
import cn.pandadb.kernel.transaction.PandaTransaction
import cn.pandadb.kernel.util.DBNameMap
import cn.pandadb.kernel.util.log.PandaLog
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{Transaction, TransactionDB, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:13 上午 2021/8/9
 * @Modified By:
 */
class TransactionRelationStoreAPI(relationDB: TransactionDB,
                                  inRelationDB: TransactionDB,
                                  outRelationDB: TransactionDB,
                                  relationLabelDB: TransactionDB,
                                  metaDB: TransactionDB,
                                  pandaLog: PandaLog) extends TransactionRelationStoreSPI{

  private val relationStore = new TransactionRelationPropertyStore(relationDB, pandaLog)
  private val inRelationStore = new TransactionRelationDirectionStore(inRelationDB, TransactionRelationDirection.IN, pandaLog)
  private val outRelationStore = new TransactionRelationDirectionStore(outRelationDB, TransactionRelationDirection.OUT, pandaLog)
  private val relationLabelStore = new TransactionRelationLabelIndex(relationLabelDB, pandaLog)
  private val relationTypeNameStore = new TransactionRelationTypeNameStore(metaDB, pandaLog)
  private val propertyName = new TransactionPropertyNameStore(metaDB, pandaLog)
  private val relationIdGenerator = new TransactionIdGenerator(relationDB, 200, pandaLog)

  override def generateTransactions(writeOptions: WriteOptions): Map[String, Transaction] = {
    Map(DBNameMap.relationDB -> relationDB.beginTransaction(writeOptions),
      DBNameMap.inRelationDB -> inRelationDB.beginTransaction(writeOptions),
      DBNameMap.outRelationDB -> outRelationDB.beginTransaction(writeOptions),
      DBNameMap.relationLabelDB -> relationLabelDB.beginTransaction(writeOptions),
      DBNameMap.relationMetaDB -> metaDB.beginTransaction(writeOptions))
  }

  override def allRelationTypes(): Array[String] = relationTypeNameStore.mapString2Int.keys.toArray

  override def allRelationTypeIds(): Array[Int] = relationTypeNameStore.mapInt2String.keys.toArray

  override def relationCount(tx: LynxTransaction): Long = relationStore.count(tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationDB))

  override def getRelationTypeName(relationTypeId: Int): Option[String] = relationTypeNameStore.key(relationTypeId)

  override def getRelationTypeId(relationTypeName: String): Option[Int] = relationTypeNameStore.id(relationTypeName)

  override def addRelationType(relationTypeName: String, tx: LynxTransaction): Int =
    relationTypeNameStore.getOrAddId(relationTypeName, tx)

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyName.id(keyName)

  override def addPropertyKey(keyName: String, tx: LynxTransaction): Int =
    propertyName.getOrAddId(keyName, tx)

  override def getRelationById(relId: Long, tx: LynxTransaction): Option[StoredRelationWithProperty] = relationStore.get(relId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationDB))

  override def getRelationIdsByRelationType(relationTypeId: Int, tx: LynxTransaction): Iterator[Long] = relationLabelStore.getRelations(relationTypeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationLabelDB))

  override def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any, tx: LynxTransaction): Unit = {
    relationStore.get(relationId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationDB)).foreach{
      rel =>
        relationStore.set(new StoredRelationWithProperty(rel.id, rel.from, rel.to, rel.typeId,
          rel.properties ++ Map(propertyKeyId->propertyValue)), tx)
    }
  }

  override def relationRemoveProperty(relationId: Long, propertyKeyId: Int, tx: LynxTransaction): Any = {
    relationStore.get(relationId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationDB)).foreach{
      rel =>
        relationStore.set(new StoredRelationWithProperty(rel.id, rel.from, rel.to, rel.typeId,
          rel.properties - propertyKeyId), tx)
    }
  }

  override def deleteRelation(relationId: Long, tx: LynxTransaction): Unit = {
    relationStore.get(relationId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationDB)).foreach{
      relation =>
        relationStore.delete(relationId, tx)
        inRelationStore.delete(relation, tx)
        outRelationStore.delete(relation, tx)
        relationLabelStore.delete(relation.typeId, relation.id, tx)
    }
  }

  override def findToNodeIds(fromNodeId: Long, tx: LynxTransaction): Iterator[Long] = outRelationStore.getNodeIds(fromNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.outRelationDB))

  override def findToNodeIds(fromNodeId: Long, relationType: Int, tx: LynxTransaction): Iterator[Long] =
    outRelationStore.getNodeIds(fromNodeId, relationType, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.outRelationDB))

  override def findFromNodeIds(toNodeId: Long, tx: LynxTransaction): Iterator[Long] = inRelationStore.getNodeIds(toNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB))

  override def findFromNodeIds(toNodeId: Long, relationType: Int, tx: LynxTransaction): Iterator[Long] =
    inRelationStore.getNodeIds(toNodeId, relationType, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB))

  override def newRelationId(): Long = relationIdGenerator.nextId()

  override def addRelation(relation: StoredRelation, tx: LynxTransaction): Unit = {
    relationStore.set(relation, tx)
    inRelationStore.set(relation, tx)
    outRelationStore.set(relation, tx)
    relationLabelStore.set(relation.typeId, relation.id, tx)
  }

  override def addRelation(relation: StoredRelationWithProperty, tx: LynxTransaction): Unit = {
    relationStore.set(relation, tx)
    inRelationStore.set(relation, tx)
    outRelationStore.set(relation, tx)
    relationLabelStore.set(relation.typeId, relation.id, tx)
  }

  override def allRelations(withProperty: Boolean, tx: LynxTransaction): Iterator[StoredRelation] = {
    if(withProperty) relationStore.all(tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.relationDB))
    else inRelationStore.all(tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB))
  }

  override def findOutRelations(fromNodeId: Long, edgeType:Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation] =
    edgeType.map(outRelationStore.getRelations(fromNodeId, _, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.outRelationDB)))
      .getOrElse(outRelationStore.getRelations(fromNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.outRelationDB)))

  override def findInRelations(toNodeId: Long, edgeType:Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation] =
    edgeType.map(inRelationStore.getRelations(toNodeId, _, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB)))
      .getOrElse(inRelationStore.getRelations(toNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB)))

  override def findInRelationsBetween(toNodeId: Long, fromNodeId: Long, edgeType: Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation] = {
    edgeType.map(inRelationStore.getRelation(toNodeId, _, fromNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB)).toIterator)
      .getOrElse(inRelationStore.getRelations(toNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.inRelationDB)).filter(r => r.from == fromNodeId))
  }

  override def findOutRelationsBetween(fromNodeId: Long, toNodeId: Long, edgeType: Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation] = {
    edgeType.map(outRelationStore.getRelation(fromNodeId, _, toNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.outRelationDB)).toIterator)
      .getOrElse(outRelationStore.getRelations(fromNodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.outRelationDB)).filter(r => r.to == toNodeId))
  }

  override def close(): Unit = {
    relationStore.close()
    inRelationStore.close()
    outRelationStore.close()
    relationLabelStore.close()
    metaDB.close()
  }
}
