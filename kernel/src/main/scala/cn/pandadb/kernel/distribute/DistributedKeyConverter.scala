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

  val nodeKeyPrefix: Byte = 1
  val nodeLabelKeyPrefix: Byte = 2
  val outRelationPrefix: Byte = 3
  val inRelationPrefix: Byte = 4
  val typeRelationPrefix: Byte = 5
  val relationKeyPrefix: Byte = 6

  def nodeMaxIdKey: Array[Byte] = "nodeMaxId".getBytes(Charset.CharsetUTF8)
  def relationMaxIdKey: Array[Byte] = "relationMaxId".getBytes(Charset.CharsetUTF8)


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

}
