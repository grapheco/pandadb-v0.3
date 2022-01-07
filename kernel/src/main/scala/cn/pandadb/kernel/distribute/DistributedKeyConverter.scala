package cn.pandadb.kernel.distribute

import cn.pandadb.kernel.distribute.relationship.RelationDirection
import cn.pandadb.kernel.distribute.relationship.RelationDirection.RelationDirection
import cn.pandadb.kernel.kv.ByteUtils
import org.tikv.common.types.Charset

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-16 10:53
 */
object DistributedKeyConverter {

  val nodeKeyPrefix: Byte = 10
  val nodeLabelKeyPrefix: Byte = 11
  val outRelationPrefix: Byte = 12
  val inRelationPrefix: Byte = 13
  val typeRelationPrefix: Byte = 14
  val relationKeyPrefix: Byte = 15

  val nodeLabelMetaPrefix: Byte = 16
  val relationTypeMetaPrefix: Byte = 17
  val propertyMetaPrefix: Byte = 18

  def nodeMaxIdKey: Array[Byte] = Array(19.toByte)
  def relationMaxIdKey: Array[Byte] = Array(20.toByte)

  val indexMetaPrefix: Byte = 21
  val statisticPrefix: Byte = 22

  val indexEncoderPrefix: Byte = 23

  val NODE_ID_SIZE     = 8
  val RELATION_ID_SIZE = 8
  val LABEL_ID_SIZE    = 4
  val TYPE_ID_SIZE     = 4
  val PROPERTY_ID_SIZE = 4
  val INDEX_ID_SIZE    = 4
  val KEY_PREFIX_SIZE = 1

  type NodeId     = Long
  type RelationId = Long
  type LabelId    = Int
  type TypeId     = Int
  type PropertyId = Int
  type IndexId    = Int

  /**
   * ╔══════════════════════╗
   * ║        key                         ║
   * ╠═════╦══════╦════════╣
   * ║ prefix ║ labelId ║ nodeId       ║
   * ╚════╩══════╩════════╝
   */

  def toNodeKey(labelId: LabelId, nodeId: NodeId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + LABEL_ID_SIZE + NODE_ID_SIZE)
    ByteUtils.setByte(bytes, 0, nodeKeyPrefix)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE, labelId)
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE + LABEL_ID_SIZE, nodeId)
    bytes
  }

  def toNodeKey(labelId: LabelId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + LABEL_ID_SIZE)
    ByteUtils.setByte(bytes, 0, nodeKeyPrefix)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE, labelId)
    bytes
  }

  def getLabelIdInNodeKey(array: Array[Byte]):LabelId = ByteUtils.getInt(array, 1)

  def getNodeIdInNodeKey(array: Array[Byte]):NodeId = ByteUtils.getLong(array, KEY_PREFIX_SIZE + LABEL_ID_SIZE)

  /**
   * ╔═══════════════════╗
   * ║        key        ║
   * ╠═════════╦═════════╣
   * ║ prefix ║ nodeId  ║ labelId ║
   * ╚═════════╩═════════╝
   */
  def toNodeLabelKey(nodeId: NodeId, labelId: LabelId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + LABEL_ID_SIZE + NODE_ID_SIZE)
    ByteUtils.setByte(bytes, 0, nodeLabelKeyPrefix)
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE, nodeId)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE + NODE_ID_SIZE, labelId)
    bytes
  }

  def toNodeLabelKey(nodeId: NodeId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + NODE_ID_SIZE)
    ByteUtils.setByte(bytes, 0, nodeLabelKeyPrefix)
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE, nodeId)
    bytes
  }

  def getNodeIdInNodeLabelKey(array: Array[Byte]):NodeId = ByteUtils.getLong(array, 1)

  def getLabelIdInNodeLabelKey(array: Array[Byte]):LabelId = ByteUtils.getInt(array, KEY_PREFIX_SIZE + NODE_ID_SIZE)


  /**
   * ╔═════════════╗
   * ║     key     ║
   * ╠═════════════╣
   * ║ prefix ║ relationId  ║
   * ╚═════════════╝
   */
  def toRelationKey(relationId: RelationId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + RELATION_ID_SIZE)
    ByteUtils.setByte(bytes, 0, relationKeyPrefix)
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE, relationId)
    bytes
  }
  /**
   * ╔═════════════════════╗
   * ║         key         ║
   * ╠════════╦════════════╣
   * ║ prefix ║ typeId ║ relationId ║
   * ╚════════╩════════════╝
   */
  def toRelationTypeKey(typeId: TypeId, relationId: RelationId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + TYPE_ID_SIZE + RELATION_ID_SIZE)
    ByteUtils.setByte(bytes, 0, typeRelationPrefix)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE, typeId)
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE + TYPE_ID_SIZE, relationId)
    bytes
  }
  def toRelationTypeKey(typeId: TypeId): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + TYPE_ID_SIZE)
    ByteUtils.setByte(bytes, 0, typeRelationPrefix)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE, typeId)
    bytes
  }

  def edgeKeyToBytes(startId: Long, typeId: Int, endNodeId: Long, direction: RelationDirection): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + NODE_ID_SIZE + TYPE_ID_SIZE + NODE_ID_SIZE)
    direction match {
      case RelationDirection.OUT =>ByteUtils.setByte(bytes, 0, outRelationPrefix)
      case RelationDirection.IN => ByteUtils.setByte(bytes, 0, inRelationPrefix)
    }
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE, startId)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE + NODE_ID_SIZE, typeId)
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE + NODE_ID_SIZE + TYPE_ID_SIZE, endNodeId)
    bytes
  }
  def edgeKeyPrefixToBytes(startId: Long, direction: RelationDirection): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + NODE_ID_SIZE)
    direction match {
      case RelationDirection.OUT =>ByteUtils.setByte(bytes, 0, outRelationPrefix)
      case RelationDirection.IN => ByteUtils.setByte(bytes, 0, inRelationPrefix)
    }
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE, startId)
    bytes
  }
  def edgeKeyPrefixToBytes(startId: Long, typeId: Int, direction: RelationDirection): Array[Byte] = {
    val bytes = new Array[Byte](KEY_PREFIX_SIZE + NODE_ID_SIZE + TYPE_ID_SIZE)
    direction match {
      case RelationDirection.OUT =>ByteUtils.setByte(bytes, 0, outRelationPrefix)
      case RelationDirection.IN => ByteUtils.setByte(bytes, 0, inRelationPrefix)
    }
    ByteUtils.setLong(bytes, KEY_PREFIX_SIZE, startId)
    ByteUtils.setInt(bytes, KEY_PREFIX_SIZE + NODE_ID_SIZE, typeId)
    bytes
  }

  // meta
  // [keyPrefix(1byte), id(4Bytes)]
  def nodeLabelKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, nodeLabelMetaPrefix)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // [keyType(1Bytes),--]
  def nodeLabelKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, nodeLabelMetaPrefix)
    bytes
  }

  // [keyPrefix(1Bytes),id(4Bytes)]
  def relationTypeKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, relationTypeMetaPrefix)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // [keyPrefix(1Bytes),--]
  def relationTypeKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, relationTypeMetaPrefix)
    bytes
  }

  // [keyPrefix(1Bytes),id(4Bytes)]
  def propertyNameKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, propertyMetaPrefix)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // [keyPrefix(1Bytes),--]
  def propertyNameKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, propertyMetaPrefix)
    bytes
  }

  // [keyPrefix(1Bytes),id(4Bytes),id(4Bytes)]
  def indexMetaToBytes(labelId: Int, propertyId: Int): Array[Byte] ={
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, indexMetaPrefix)
    ByteUtils.setInt(bytes, 1, labelId)
    ByteUtils.setInt(bytes, 5, propertyId)
    bytes
  }

  // [keyPrefix(1Bytes),--]
  def indexMetaPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, indexMetaPrefix)
    bytes
  }
  // [keyPrefix(1Bytes),--]
  def indexMetaWithLabelPrefixToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, indexMetaPrefix)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  def indexEncoderKeyToBytes(encoderName: String): Array[Byte] = {
    val nameBytes = encoderName.getBytes("utf-8")
    Array(indexEncoderPrefix) ++ nameBytes
  }

}
