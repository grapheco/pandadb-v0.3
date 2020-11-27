package cn.pandadb.kernel.kv

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import collection.JavaConverters._

object KeyHandler {
  object KeyType extends Enumeration {
    type KeyType = Value
    val Node = Value(1)
    val InEdge = Value(2)
    val OutEdge = Value(3)
    val NodeLabelIndex = Value(4)
    val NodePropertyIndex = Value(5)

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

  def nodeLabelIndexKeyToBytes(labelId: Int, nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    ByteUtils.setLong(bytes, 5, nodeId)
    bytes
  }

  def nodePropertyIndexKeyToBytes(indexId:Long, indexValue: Long, nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](25)
    ByteUtils.setByte(bytes, 0, KeyType.NodePropertyIndex.id.toByte)
    ByteUtils.setLong(bytes, 1, indexId)
    ByteUtils.setLong(bytes, 9, indexValue)
    ByteUtils.setLong(bytes, 17, nodeId)
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

}