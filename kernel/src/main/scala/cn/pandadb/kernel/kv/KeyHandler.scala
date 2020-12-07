package cn.pandadb.kernel.kv

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.{Charset, StandardCharsets}

import cn.pandadb.kernel.kv.KeyHandler.KeyType.{KeyType, Value}

class IllegalKeyException(s: String) extends RuntimeException(s) {
}

object KeyHandler {
  object KeyType extends Enumeration {
    type KeyType = Value

    val Node = Value(1)     // [keyType(1Byte),nodeId(8Bytes)] -> nodeValue(id, labels, properties)
    val NodeLabelIndex = Value(2)     // [keyType(1Byte),labelId(4Bytes),nodeId(8Bytes)] -> null
    val NodePropertyIndexMeta = Value(3)     // [keyType(1Byte),labelId(4Bytes),properties(x*4Bytes)] -> null
    val NodePropertyIndex = Value(4)  // // [keyType(1Bytes),indexId(4),propValue(xBytes),valueLength(xBytes),nodeId(8Bytes)] -> null

    val Relation = Value(5)  // [keyType(1Byte),relationId(8Bytes)] -> relationValue(id, fromNode, toNode, labelId, category)
    val RelationLabelIndex = Value(6) // [keyType(1Byte),labelId(4Bytes),relationId(8Bytes)] -> null
    val OutEdge = Value(7)  // [keyType(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),toNodeId(8Bytes)] -> relationValue(id,properties)
    val InEdge = Value(8)   // [keyType(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)] -> relationValue(id,properties)
    val NodePropertyFulltextIndexMeta = Value(9)     // [keyType(1Byte),labelId(4Bytes),properties(x*4Bytes)] -> null

    val NodeLabel = Value(10) // [KeyType(1Byte), LabelId(4Byte)] --> LabelName(String)
    val RelationLabel = Value(11) // [KeyType(1Byte), LabelId(4Byte)] --> LabelName(String)
    val PropertyName = Value(12) // [KeyType(1Byte),PropertyId(4Byte)] --> propertyName(String)
  }
////////////////
  //  [keyType(1Bytes),  id(4Bytes)]
  def nodeLabelKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabel.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  def nodeLabelKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabel.id.toByte)
    bytes
  }

  //  [keyType(1Bytes),  id(4Bytes)]
  def relationLabelKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.RelationLabel.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  def relationLabelKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.RelationLabel.id.toByte)
    bytes
  }

  //  [keyType(1Bytes),  id(4Bytes)]
  def propertyNameKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.PropertyName.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  def propertyNameKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.PropertyName.id.toByte)
    bytes
  }

////////////////
  // [keyType(1Byte),nodeId(8Bytes)]
  def nodeKeyToBytes(nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, KeyType.Node.id.toByte)
    ByteUtils.setLong(bytes, 1, nodeId)
    bytes
  }

  // [keyType(1Byte),--]
  def nodeKeyPrefix(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.Node.id.toByte)
    bytes
  }

  // return (nodeId:Long)
  def parseNodeKeyFromBytes(bytes: Array[Byte]): (Long) = {
    if (bytes.length<9) throw new IllegalKeyException("The length of key bytes is less than required.")
    val keyType = ByteUtils.getByte(bytes, 0)
    if (keyType.toInt != KeyType.Node.id) throw new IllegalKeyException("Illegal keyType.")
    (ByteUtils.getLong(bytes, 1))
  }

  // [keyType(1Byte),labelId(4Bytes),nodeId(8Bytes)]
  def nodeLabelIndexKeyToBytes(labelId: Int, nodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    ByteUtils.setLong(bytes, 5, nodeId)
    bytes
  }

  // [keyType(1Byte),--,--]
  def nodeLabelIndexKeyPrefixToBytes(labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // return (labelId(Int),nodeId:Long)
  def parseNodeLabelIndexKeyFromBytes(bytes: Array[Byte]): (Int, Long) = {
    if (bytes.length<13) throw new IllegalKeyException("The length of key bytes is less than required.")
    val keyType = ByteUtils.getByte(bytes, 0)
    if (keyType.toInt != KeyType.NodeLabelIndex.id) throw new IllegalKeyException("Illegal keyType.")
    (ByteUtils.getInt(bytes, 1), ByteUtils.getLong(bytes, 5))
  }

  // [keyType(1Byte),labelId(4Bytes),properties(x*4Bytes)]
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

  // [keyType(1Byte),labelId(4Bytes),properties(x*4Bytes)]
  def nodePropertyFulltextIndexMetaKeyToBytes(labelId: Int, props:Array[Int]): Array[Byte] = {
    val bytes = new Array[Byte](1+4+4*props.length)
    ByteUtils.setByte(bytes, 0, KeyType.NodePropertyFulltextIndexMeta.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    var index=5
    props.foreach{
      p=>ByteUtils.setInt(bytes,index,p)
        index+=4
    }
    bytes
  }

  // [keyType(1Bytes),indexId(4),propValue(xBytes),valueLength(xBytes),nodeId(8Bytes)]
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

  // [keyType(1Bytes),indexId(4),propValue(xBytes),valueLength(xBytes),nodeId(8Bytes)]
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

  // [keyType(1Byte),relationId(8Bytes)]
  def relationKeyToBytes(relationId: Long): Array[Byte] = {
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, KeyType.Relation.id.toByte)
    ByteUtils.setLong(bytes, 1, relationId)
    bytes
  }

  // [keyType(1Byte),--]
  def relationKeyPrefix(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.Relation.id.toByte)
    bytes
  }

  // return (relationId:Long)
  def parseRelationKeyFromBytes(bytes: Array[Byte]): (Long) = {
    if (bytes.length<9) throw new IllegalKeyException("The length of key bytes is less than required.")
    val keyType = ByteUtils.getByte(bytes, 0)
    if (keyType.toInt != KeyType.Relation.id) throw new IllegalKeyException("Illegal keyType.")
    (ByteUtils.getLong(bytes, 1))
  }

  // [keyType(1Byte),relLabelId(4Bytes),relationId(8Bytes)]
  def relationLabelIndexKeyToBytes(labelId: Int, relationId: Long): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.RelationLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    ByteUtils.setLong(bytes, 5, relationId)
    bytes
  }

  // [keyType(1Byte),relLabelId(4Bytes),--]
  def relationLabelIndexKeyPrefixToBytes(labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.RelationLabelIndex.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // return (relLabelId(Int),relationId:Long)
  def parseRelationLabelIndexKeyFromBytes(bytes: Array[Byte]): (Int,Long) = {
    if (bytes.length<13) throw new IllegalKeyException("The length of key bytes is less than required.")
    val keyType = ByteUtils.getByte(bytes, 0)
    if (keyType.toInt != KeyType.RelationLabelIndex.id) throw new IllegalKeyException("Illegal keyType.")
    (ByteUtils.getInt(bytes, 1), ByteUtils.getLong(bytes, 5))
  }

  // [keyType(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),toNodeId(8Bytes)]
  def outEdgeKeyToBytes(fromNodeId: Long, labelId: Int, category: Long, toNodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](29)
    ByteUtils.setByte(bytes, 0, KeyType.OutEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, fromNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    ByteUtils.setLong(bytes, 13, category)
    ByteUtils.setLong(bytes, 21, toNodeId)
    bytes
  }

  // return (fromNodeId: Long, labelId: Int, category: Long, toNodeId: Long)
  def parseOutEdgeKeyFromBytes(bytes: Array[Byte]): (Long,Int,Long,Long) = {
    if (bytes.length<29) throw new IllegalKeyException("The length of key bytes is less than required.")
    val keyType = ByteUtils.getByte(bytes, 0)
    if (keyType.toInt != KeyType.OutEdge.id) throw new IllegalKeyException("Illegal keyType.")
    ( ByteUtils.getLong(bytes, 1),
    ByteUtils.getInt(bytes, 9),
    ByteUtils.getLong(bytes, 13),
    ByteUtils.getLong(bytes, 21) )
  }

  // [keyType(1Byte),--,--,--,--]
  def outEdgeKeyPrefixToBytes(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.OutEdge.id.toByte)
    bytes
  }

  // [keyType(1Byte),fromNodeId(8Bytes),--,--,--]
  def outEdgeKeyPrefixToBytes(fromNodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, KeyType.OutEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, fromNodeId)
    bytes
  }

  // [keyType(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),--,--]
  def outEdgeKeyPrefixToBytes(fromNodeId: Long, labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.OutEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, fromNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    bytes
  }

  // [keyType(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),--]
  def outEdgeKeyPrefixToBytes(fromNodeId: Long, labelId: Int, category: Long): Array[Byte] = {
    val bytes = new Array[Byte](21)
    ByteUtils.setByte(bytes, 0, KeyType.OutEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, fromNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    ByteUtils.setLong(bytes, 13, category)
    bytes
  }

  // [keyType(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)]
  def inEdgeKeyToBytes(toNodeId: Long, labelId: Int, category: Long, fromNodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](29)
    ByteUtils.setByte(bytes, 0, KeyType.InEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, toNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    ByteUtils.setLong(bytes, 13, category)
    ByteUtils.setLong(bytes, 21, fromNodeId)
    bytes
  }

  // return (toNodeId: Long, labelId: Int, category: Long, fromNodeId: Long)
  def parseInEdgeKeyFromBytes(bytes: Array[Byte]): (Long,Int,Long,Long) = {
    if (bytes.length<29) throw new IllegalKeyException("The length of key bytes is less than required.")
    val keyType = ByteUtils.getByte(bytes, 0)
    if (keyType.toInt != KeyType.InEdge.id) throw new IllegalKeyException("Illegal keyType.")
    (ByteUtils.getLong(bytes, 1),
      ByteUtils.getInt(bytes, 9),
    ByteUtils.getLong(bytes, 13),
    ByteUtils.getLong(bytes, 21) )
  }

  // [keyType(1Byte),--,--,--,--]
  def inEdgeKeyPrefixToBytes(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.InEdge.id.toByte)
    bytes
  }

  // [keyType(1Byte),toNodeId(8Bytes),--,--,--]
  def inEdgeKeyPrefixToBytes(toNodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](9)
    ByteUtils.setByte(bytes, 0, KeyType.InEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, toNodeId)
    bytes
  }

  // [keyType(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),--,--]
  def inEdgeKeyPrefixToBytes(toNodeId: Long, labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](13)
    ByteUtils.setByte(bytes, 0, KeyType.InEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, toNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    bytes
  }

  // [keyType(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),--]
  def inEdgeKeyPrefixToBytes(toNodeId: Long, labelId: Int, category: Long): Array[Byte] = {
    val bytes = new Array[Byte](21)
    ByteUtils.setByte(bytes, 0, KeyType.InEdge.id.toByte)
    ByteUtils.setLong(bytes, 1, toNodeId)
    ByteUtils.setInt(bytes, 9, labelId)
    ByteUtils.setLong(bytes, 13, category)
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
  def labelMapToBytes(map: Map[Int, String]): Array[Byte] = {
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
  def labelMapFromBytes(bytes: Array[Byte]): Map[Int, String] = {
    val bis=new ByteArrayInputStream(bytes)
    val ois=new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[Map[Int, String]]
  }

  def longToBytes(num: Long): Array[Byte] = {
    val bytes = new Array[Byte](8)
    ByteUtils.setLong(bytes, 0, num)
    bytes
  }
  def intToBytes(num: Int): Array[Byte] = {
    val bytes = new Array[Byte](4)
    ByteUtils.setInt(bytes, 0, num)
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