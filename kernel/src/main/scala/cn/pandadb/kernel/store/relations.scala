package cn.pandadb.kernel.store

import cn.pandadb.kernel.util.log.LogWriter
import org.grapheco.lynx.{LynxId, LynxRelationship, LynxTransaction, LynxValue}
import org.rocksdb.{Transaction, WriteOptions}


case class StoredRelation(id: Long, from: Long, to: Long, typeId: Int) extends StoredValue{
  val properties:Map[Int,Any] = Map.empty
}

class StoredRelationWithProperty(override val id: Long,
                                 override val from: Long,
                                 override val to: Long,
                                 override val typeId: Int,
                                 override val properties:Map[Int,Any])
  extends StoredRelation(id, from, to, typeId) {
}

case class RelationId(value: Long) extends LynxId {}

case class PandaRelationship(_id: Long, startId: Long, endId: Long, relationType: Option[String],
                            props: (String, LynxValue)*) extends LynxRelationship {
  lazy val properties = props.toMap
  override val id: LynxId = RelationId(_id)
  override val startNodeId: LynxId = NodeId(startId)
  override val endNodeId: LynxId = NodeId(endId)

  override def property(name: String): Option[LynxValue] = properties.get(name)
}


trait RelationStoreSPI {
  def allRelationTypes(): Array[String];

  def allRelationTypeIds(): Array[Int];

  def relationCount: Long

  def getRelationTypeName(relationTypeId: Int): Option[String];

  def getRelationTypeId(relationTypeName: String): Option[Int];

  def addRelationType(relationTypeName: String): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def addPropertyKey(keyName: String): Int;

  def getRelationById(relId: Long): Option[StoredRelationWithProperty];

  def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long];

  def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any;

  def deleteRelation(relationId: Long): Unit;

  def findToNodeIds(fromNodeId: Long): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];

  def findFromNodeIds(toNodeId: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];

  def newRelationId(): Long;

  def addRelation(relation: StoredRelation): Unit

  def addRelation(relation: StoredRelationWithProperty): Unit

  def allRelations(withProperty: Boolean = false): Iterator[StoredRelation]

  def findOutRelations(fromNodeId: Long): Iterator[StoredRelation] = findOutRelations(fromNodeId, None)

  def findOutRelations(fromNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def findInRelations(toNodeId: Long): Iterator[StoredRelation] = findInRelations(toNodeId, None)

  def findInRelations(toNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def findInRelationsBetween(toNodeId: Long, fromNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def findOutRelationsBetween(fromNodeId: Long, toNodeId: Long, edgeType: Option[Int] = None): Iterator[StoredRelation]

  def close(): Unit
}

trait TransactionRelationStoreSPI {
  def generateTransactions(writeOptions: WriteOptions): Map[String, Transaction];

  def allRelationTypes(): Array[String];

  def allRelationTypeIds(): Array[Int];

  def relationCount(tx: LynxTransaction): Long

  def getRelationTypeName(relationTypeId: Int): Option[String];

  def getRelationTypeId(relationTypeName: String): Option[Int];

  def addRelationType(relationTypeName: String, tx: LynxTransaction, logWriter: LogWriter): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def addPropertyKey(keyName: String, tx: LynxTransaction, logWriter: LogWriter): Int;

  def getRelationById(relId: Long, tx: LynxTransaction): Option[StoredRelationWithProperty];

  def getRelationIdsByRelationType(relationTypeId: Int, tx: LynxTransaction): Iterator[Long];

  def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any, tx: LynxTransaction, logWriter: LogWriter): Unit;

  def relationRemoveProperty(relationId: Long, propertyKeyId: Int, tx: LynxTransaction, logWriter: LogWriter): Any;

  def deleteRelation(relationId: Long, tx: LynxTransaction, logWriter: LogWriter): Unit;

  def findToNodeIds(fromNodeId: Long, tx: LynxTransaction): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int, tx: LynxTransaction): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, tx: LynxTransaction): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int, tx: LynxTransaction): Iterator[Long];

  def newRelationId(): Long;

  def addRelation(relation: StoredRelation, tx: LynxTransaction, logWriter: LogWriter): Unit

  def addRelation(relation: StoredRelationWithProperty, tx: LynxTransaction, logWriter: LogWriter): Unit

  def allRelations(withProperty: Boolean = false, tx: LynxTransaction): Iterator[StoredRelation]

  def findOutRelations(fromNodeId: Long, tx: LynxTransaction): Iterator[StoredRelation] = findOutRelations(fromNodeId, None, tx)

  def findOutRelations(fromNodeId: Long, edgeType: Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation]

  def findInRelations(toNodeId: Long, tx: LynxTransaction): Iterator[StoredRelation] = findInRelations(toNodeId, None, tx)

  def findInRelations(toNodeId: Long, edgeType: Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation]

  def findInRelationsBetween(toNodeId: Long, fromNodeId: Long, edgeType: Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation]

  def findOutRelationsBetween(fromNodeId: Long, toNodeId: Long, edgeType: Option[Int] = None, tx: LynxTransaction): Iterator[StoredRelation]

  def close(): Unit
}