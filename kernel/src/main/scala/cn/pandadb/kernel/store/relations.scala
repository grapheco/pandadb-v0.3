package cn.pandadb.kernel.store

import org.grapheco.lynx.{LynxId, LynxRelationship, LynxValue}


//trait ReadOnlyRelation {
//  def getId(): Long;
//  def getProperty(key: String): Any;
//  def getAllProperties(): Map[String, Any];
//  def getRelationType(): String;
//  def getFromNodeId(): Long;
//  def getToNodeId(): Long;
//}
//
//trait WritableRelation extends ReadOnlyRelation{
//  def setProperty(key:String, value: Any): Unit ;
//  def removeProperty(key:String): Any;
//  def setRelationType(relationType: String): Unit;
//  def removeRelationType(relationType: String): Unit;
//}

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


//class RelationInfo(id: Long, relationTypeId: Int, fromNodeId: Long, toNodeId: Long,
//                             relStoreSPI: RelationStoreSPI)
//  extends ReadOnlyRelation {
//
//  lazy val propertyMap: Map[Int, Any] = relStoreSPI.getRelationProperties(id)
//
//  def getRelationTypeId(): Int = this.relationTypeId
//
//  def getProperties(): Map[Int, Any] = this.propertyMap
//
//  override def getId(): Long = this.id
//
//  override def getProperty(key: String): Any = propertyMap.get(relStoreSPI.getPropertyKeyId(key))
//
//  override def getAllProperties(): Map[String, Any] = propertyMap.map(kv => (relStoreSPI.getPropertyKeyName(kv._1), kv._2))
//
//  override def getRelationType(): String = relStoreSPI.getRelationTypeName(this.relationTypeId)
//
//  override def getFromNodeId(): Long = this.fromNodeId
//
//  override def getToNodeId(): Long = this.toNodeId
//}


//class RelationInfoWithProperty(id: Long,
//                                   relationTypeId: Int,
//                                   fromNodeId: Long,
//                                   toNodeId: Long,
//                                   override val propertyMap: Map[Int, Any],
//                                   relStoreSPI: RelationStoreSPI)
//  extends RelationInfo(id, relationTypeId, fromNodeId, toNodeId, relStoreSPI) {
//
//}


//class LazyRelation(id: Long, relStoreSPI: RelationStoreSPI) extends ReadOnlyRelation {
//
//  lazy val relationshipContents: Array[Byte] = relStoreSPI.getRelationInfoBytesById(this.id)
//
//  lazy val relationshipInfo: RelationInfo = relStoreSPI.deserializeBytesToRelation(relationshipContents)
//
//  lazy val relationTypeId: Int = relationshipInfo.getRelationTypeId()
//
//  lazy val propertyMap: Map[Int, Any] = relationshipInfo.getProperties()
//
//  override def getId(): Long = this.id
//
//  override def getProperty(key: String): Any = propertyMap.get(relStoreSPI.getPropertyKeyId(key))
//
//  override def getAllProperties(): Map[String, Any] = {
//    propertyMap.map(kv => (relStoreSPI.getPropertyKeyName(kv._1), kv._2))
//  }
//
//  override def getRelationType(): String = relStoreSPI.getRelationTypeName(relationshipInfo.getRelationTypeId())
//
//  override def getFromNodeId(): Long = relationshipInfo.getFromNodeId()
//
//  override def getToNodeId(): Long = relationshipInfo.getToNodeId()
//}
//
//class LazyWritableRelation(id: Long, relStoreSPI: RelationStoreSPI) extends LazyRelation(id, relStoreSPI) with WritableRelation {
//  override def setProperty(key:String, value: Any): Unit = {
//    relStoreSPI.relationSetProperty(id, relStoreSPI.getPropertyKeyId(key), value)
//  }
//
//  override def removeProperty(key:String): Any = {
//    relStoreSPI.relationRemoveProperty(id, relStoreSPI.getPropertyKeyId(key))
//  }
//
//  override def setRelationType(relationType: String): Unit = {
//    relStoreSPI.relationSetRelationType(id, relStoreSPI.getRelationTypeId(relationType))
//  }
//
//  override def removeRelationType(relationType: String): Unit = {
//    relStoreSPI.relationRemoveRelationType(id, relStoreSPI.getRelationTypeId(relationType))
//  }
//}
//

trait RelationStoreSPI {
  def allRelationTypes(): Array[String];

  def allRelationTypeIds(): Array[Int];

  def relationCount: Long

  def getRelationTypeName(relationTypeId: Int): Option[String];

  def getRelationTypeId(relationTypeName: String): Int;

  def addRelationType(relationTypeName: String): Int;

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Int;

  def addPropertyKey(keyName: String): Int;

  //  def getRelationById(relId: Long): Array[Byte];
  //  def getRelationProperties(relId: Long): Map[Int, Any];
  def getRelationById(relId: Long): Option[StoredRelationWithProperty];

  def getRelationIdsByRelationType(relationTypeId: Int): Iterator[Long];

  //  def relationSetRelationType(relationId: Long, relationTypeId:Int): Unit;
  //  def relationRemoveRelationType(relationId: Long, relationTypeId:Int): Unit;

  def relationSetProperty(relationId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def relationRemoveProperty(relationId: Long, propertyKeyId: Int): Any;

  def deleteRelation(relationId: Long): Unit;

  //  def serializeRelationToBytes(relationship: StoredRelationWithProperty): Array[Byte];
  //  def deserializeBytesToRelation(bytes: Array[Byte]): StoredRelationWithProperty;

  def findToNodeIds(fromNodeId: Long): Iterator[Long];

  def findToNodeIds(fromNodeId: Long, relationType: Int): Iterator[Long];

  //  def findToNodeIds(fromNodeId: Long, relationType: Int, category: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long): Iterator[Long];

  def findFromNodeIds(toNodeId: Long, relationType: Int): Iterator[Long];

  //  def findFromNodeIds(toNodeId: Long, relationType: Int, category: Long): Iterator[Long];

  def newRelationId(): Long;

  def addRelation(relation: StoredRelation): Unit

  def addRelation(relation: StoredRelationWithProperty): Unit

  def allRelations(withProperty: Boolean = false): Iterator[StoredRelation]

  def findOutRelations(fromNodeId: Long): Iterator[StoredRelation]

  def findOutRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelation]

  def findInRelations(toNodeId: Long): Iterator[StoredRelation]

  def findInRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelation]

  def close(): Unit
}