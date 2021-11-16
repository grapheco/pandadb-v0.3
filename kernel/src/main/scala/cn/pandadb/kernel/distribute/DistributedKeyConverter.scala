package cn.pandadb.kernel.distribute

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

}
