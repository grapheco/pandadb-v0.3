package cn.pandadb.kernel.kv

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.{Charset, StandardCharsets}

import cn.pandadb.kernel.kv.KeyHandler.KeyType.KeyType

object KeyHandler {
  object KeyType extends Enumeration {
    type KeyType = Value

    val Node = Value(1)     // [type(1Byte),nodeId(8Bytes)] -> nodeValue(id, labels, properties)
    val NodeLabelIndex = Value(2)     // [type(1Byte),labelId(4Bytes),nodeId(8Bytes)] -> null
    val NodePropertyIndexMeta = Value(3)     // [type(1Byte),labelId(4Bytes),properties(8Bytes)] -> null
    val NodePropertyIndex = Value(4)  // [type(1Bytes),indexId(4),indexValue(xBytes)]

    val Relation = Value(5)  // [type(1Byte),relationId(8Bytes)] -> relationValue(id, fromNode, toNode, labelId, category)
    val RelationLabelIndex = Value(6) // [type(1Byte),labelId(4Bytes),relationId(8Bytes)] -> null
    val InEdge = Value(7)   // [type(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),toNodeId(8Bytes)] -> relationValue(properties)
    val OutEdge = Value(8)  // [type(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)] -> relationValue(properties)
  }

  def nodeKeyToBytes(nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, KeyType.Node.id.toByte)
    ByteUtils.setLong(bytes, 1, nodeId)
    bytes
  }

  def nodeKeyPrefix(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.Node.id.toByte)
    bytes
  }

  def inEdgeKeyToBytes(): Array[Byte] ={
    val array = Array[Byte](1)
    ByteUtils.setByte(array, 0, KeyType.InEdge.id.toByte)
    array
  }
  def outEdgeKeyToBytes(): Array[Byte] ={
    val array = Array[Byte](1)
    ByteUtils.setByte(array, 0, KeyType.OutEdge.id.toByte)
    array
  }

  def relationKeyToBytes(relationId: Long): Array[Byte] = {
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, KeyType.Relation.id.toByte)
    ByteUtils.setLong(bytes, 1, relationId)
    bytes
  }

  def relationKeyPrefix(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.Relation.id.toByte)
    bytes
  }

  def relationIndexPrefixKeyToBytes(rType: Byte, NodeId: Long, edgeType: Int): Array[Byte] ={
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, rType)
    ByteUtils.setLong(bytes, 1, NodeId)
    ByteUtils.setInt(bytes, 9, edgeType)
    bytes
  }
  // for category
  def relationIndexPrefixKeyToBytes(rType: Byte, NodeId: Long): Array[Byte] ={
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, rType)
    ByteUtils.setLong(bytes, 1, NodeId)
    bytes
  }

  def relationIndexPrefixKeyToBytes(rType: Byte, NodeId: Long, edgeType: Int, category: Long): Array[Byte] ={
    val bytes = new Array[Byte](21)
    ByteUtils.setByte(bytes, 0, rType)
    ByteUtils.setLong(bytes, 1, NodeId)
    ByteUtils.setInt(bytes, 9, edgeType)
    ByteUtils.setLong(bytes, 13, category)
    bytes
  }

  def inEdgeKeyToBytes(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Array[Byte] = {
    val bytes = new Array[Byte](29)
    ByteUtils.setByte(bytes, 0, KeyType.InEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, fromNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    ByteUtils.setLong(bytes, 13, category)
    ByteUtils.setLong(bytes, 21, toNodeId)
    bytes
  }

  def outEdgeKeyToBytes(fromNodeId: Long, toNodeId: Long, labelId: Int, category: Long): Array[Byte] = {
    val bytes = new Array[Byte](29)
    ByteUtils.setByte(bytes, 0, KeyType.OutEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, toNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    ByteUtils.setLong(bytes, 13, category)
    ByteUtils.setLong(bytes, 21, fromNodeId)
    bytes
  }

  def bytesToEdge(byteArr: Array[Byte]): (Byte, Long, Int, Long, Long) = {
    (
      ByteUtils.getByte(byteArr, 0),
      ByteUtils.getLong(byteArr, 1),
      ByteUtils.getInt(byteArr, 9),
      ByteUtils.getLong(byteArr,13),
      ByteUtils.getLong(byteArr,21)
    )
  }

  def nodeLabelIndexKeyToBytes(labelId: Int, nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    ByteUtils.setLong(bytes, 5, nodeId)
    bytes
  }

  def nodeLabelIndexKeyPrefixToBytes(labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  def nodePropertyIndexMetaKeyToBytes(labelId: Int, props:Array[Int]): Array[Byte] = {
    val bytes = new Array[Byte](1+4+4*props.length)
    ByteUtils.setByte(bytes, 0, KeyType.NodePropertyIndexMeta.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    var index=5
    props.foreach{
      p=>ByteUtils.setInt(bytes,index,p)
        index+=4
    }
    bytes
  }

  def nodePropertyIndexKeyToBytes(indexId:Int, value: Array[Byte], length: Array[Byte], nodeId: Long): Array[Byte] = {
    val bytesLength = 13+value.length+length.length
    val bytes = new Array[Byte](bytesLength)
    ByteUtils.setByte(bytes, 0, KeyType.NodePropertyIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, indexId)
    for (i <- value.indices)
      bytes(5+i) = value(i)
    for (i <- length.indices)
      bytes(5+value.length+i) = length(i)
    ByteUtils.setLong(bytes, bytesLength-8, nodeId)
    bytes
  }

  def nodePropertyIndexPrefixToBytes(indexId:Int, value: Array[Byte], length: Array[Byte]): Array[Byte] = {
    val bytesLength = 5+value.length+length.length
    val bytes = new Array[Byte](bytesLength)
    ByteUtils.setByte(bytes, 0, KeyType.NodePropertyIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, indexId)
    for (i <- value.indices)
      bytes(5+i) = value(i)
    for (i <- length.indices)
      bytes(5+value.length+i) = length(i)
    bytes
  }

  def relationLabelIndexKeyToBytes(labelId: Int, nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    ByteUtils.setLong(bytes, 5, nodeId)
    bytes
  }

  def relationLabelIndexKeyPrefixToBytes(labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }
}

object ByteUtils {
  def setLong(bytes: Array[Byte], index: Int, value: Long): Unit = {
    bytes(index) = (value >>> 56).toByte
    bytes(index + 1) = (value >>> 48).toByte
    bytes(index + 2) = (value >>> 40).toByte
    bytes(index + 3) = (value >>> 32).toByte
    bytes(index + 4) = (value >>> 24).toByte
    bytes(index + 5) = (value >>> 16).toByte
    bytes(index + 6) = (value >>> 8).toByte
    bytes(index + 7) = value.toByte
  }

  def getLong(bytes: Array[Byte], index: Int): Long = {
    (bytes(index).toLong & 0xff) << 56 |
      (bytes(index + 1).toLong & 0xff) << 48 |
      (bytes(index + 2).toLong & 0xff) << 40 |
      (bytes(index + 3).toLong & 0xff) << 32 |
      (bytes(index + 4).toLong & 0xff) << 24 |
      (bytes(index + 5).toLong & 0xff) << 16 |
      (bytes(index + 6).toLong & 0xff) << 8 |
      bytes(index + 7).toLong & 0xff
  }

  def setInt(bytes: Array[Byte], index: Int, value: Int): Unit = {
    bytes(index) = (value >>> 24).toByte
    bytes(index + 1) = (value >>> 16).toByte
    bytes(index + 2) = (value >>> 8).toByte
    bytes(index + 3) = value.toByte
  }

  def getInt(bytes: Array[Byte], index: Int): Int = {
    (bytes(index) & 0xff) << 24 |
      (bytes(index + 1) & 0xff) << 16 |
      (bytes(index + 2) & 0xff) << 8 |
      bytes(index + 3) & 0xff
  }

  def setShort(bytes: Array[Byte], index: Int, value: Short): Unit = {
    bytes(index) = (value >>> 8).toByte
    bytes(index + 1) = value.toByte
  }

  def getShort(bytes: Array[Byte], index: Int): Short = {
    (bytes(index) << 8 | bytes(index + 1) & 0xFF).toShort
  }

  def setByte(bytes: Array[Byte], index: Int, value: Byte): Unit = {
    bytes(index) = value
  }

  def getByte(bytes: Array[Byte], index: Int): Byte = {
    bytes(index)
  }

  def mapToBytes(map: Map[String, Any]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(map)
    oos.close()
    bos.toByteArray
  }

  def mapFromBytes(bytes: Array[Byte]): Map[String, Any] = {
    val bis=new ByteArrayInputStream(bytes)
    val ois=new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[Map[String, Any]]
  }

  def longToBytes(num: Long): Array[Byte] = {
    val bytes = new Array[Byte](8)
    ByteUtils.setLong(bytes, 0, num)
    bytes
  }


  def stringToBytes(str: String, charset: Charset = StandardCharsets.UTF_8): Array[Byte] = {
    str.getBytes(charset)
  }

  def stringFromBytes(bytes: Array[Byte], offset: Int = 0, length:Int = 0,
                      charset: Charset = StandardCharsets.UTF_8): String = {
    if (length > 0) new String(bytes, offset, length, charset)
    else new String(bytes, offset, bytes.length, charset)
  }
}