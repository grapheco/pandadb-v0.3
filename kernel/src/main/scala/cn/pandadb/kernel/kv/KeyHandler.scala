package cn.pandadb.kernel.kv

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.{Charset, StandardCharsets}

import cn.pandadb.kernel.kv.KeyHandler.KeyType.{KeyType, Value}
import cn.pandadb.kernel.util.serializer.NodeSerializer

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
    val RelationType = Value(11) // [KeyType(1Byte), LabelId(4Byte)] --> LabelName(String)
    val PropertyName = Value(12) // [KeyType(1Byte),PropertyId(4Byte)] --> propertyName(String)

    val NewNode = Value(13)     // [keyType(1Byte),labelId(4Bytes),nodeId(8Bytes)] -> nodeValue(id, labels, properties)

  }


  // [labelId(4Bytes),nodeId(8Bytes)]
  def nodeKeyToBytes(labelId: Int, nodeId: Long): Array[Byte] = {
    NodeSerializer.serialize(labelId, nodeId)
  }

  // [labelId(4Bytes),----]
  def nodePrefix(labelId: Int): Array[Byte] = {
    ByteUtils.intToBytes(labelId)
  }

  // [nodeId(8Bytes), labelId(4Bytes)]
  def nodeLabelToBytes(nodeId: Long, labelId: Int): Array[Byte] = {
    val bytes = new Array[Byte](12)
    ByteUtils.setLong(bytes, 0, nodeId)
    ByteUtils.setInt(bytes, 8, labelId)
    bytes
  }

  // [nodeId(8Bytes), ----]
  def nodeLabelPrefix(nodeId: Long): Array[Byte] = {
    ByteUtils.longToBytes(nodeId)
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

  // [indexId(4),typeCode(1),propValue(xBytes),nodeId(8Bytes)]
  def nodePropertyIndexKeyToBytes(indexId:Int, typeCode:Byte, value: Array[Byte], nodeId: Long): Array[Byte] = {
    val bytesLength = 13 + value.length
    val bytes = new Array[Byte](bytesLength)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, 4, typeCode)
    for (i <- value.indices)
      bytes(5+i) = value(i)
    ByteUtils.setLong(bytes, bytesLength-8, nodeId)
    bytes
  }

  // [indexId(4),typeCode(1),propValue(xBytes),----]
  def nodePropertyIndexPrefixToBytes(indexId:Int, typeCode:Byte, value: Array[Byte]): Array[Byte] = {
    val bytesLength = 5 + value.length
    val bytes = new Array[Byte](bytesLength)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, 4, typeCode)
    for (i <- value.indices)
      bytes(5+i) = value(i)
    bytes
  }

  // [indexId(4),typeCode(1),----,----]
  def nodePropertyIndexTypePrefix(indexId:Int, typeCode:Byte): Array[Byte] = {
    val bytes = new Array[Byte](5)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, 4, typeCode)
    bytes
  }

  // [relationId(8Bytes)]
  def relationKeyToBytes(relationId: Long): Array[Byte] = {
    ByteUtils.longToBytes(relationId)
  }

  // [relLabelId(4Bytes),relationId(8Bytes)]
  def relationLabelIndexKeyToBytes(labelId: Int, relationId: Long): Array[Byte] = {
    val bytes = new Array[Byte](12)
    ByteUtils.setInt(bytes, 0, labelId)
    ByteUtils.setLong(bytes, 4, relationId)
    bytes
  }

  // [relLabelId(4Bytes),----]
  def relationLabelIndexKeyPrefixToBytes(labelId: Int): Array[Byte] = {
    ByteUtils.intToBytes(labelId)
  }

  // [fromNodeId(8Bytes),relationType(4Bytes),toNodeId(8Bytes)]
  def edgeKeyToBytes(fromNodeId: Long, labelId: Int, toNodeId: Long): Array[Byte] = {
    val bytes = new Array[Byte](20)
    ByteUtils.setLong(bytes, 0, fromNodeId)
    ByteUtils.setInt(bytes, 8, labelId)
    ByteUtils.setLong(bytes, 12, toNodeId)
    bytes
  }

  // [fromNodeId(8Bytes),----,----]
  def edgeKeyPrefixToBytes(fromNodeId: Long): Array[Byte] = {
    ByteUtils.longToBytes(fromNodeId)
  }

  // [fromNodeId(8Bytes),relationType(4Bytes),----]
  def edgeKeyPrefixToBytes(fromNodeId: Long, typeId: Int): Array[Byte] = {
    val bytes = new Array[Byte](12)
    ByteUtils.setLong(bytes, 0, fromNodeId)
    ByteUtils.setInt(bytes, 8, typeId)
    bytes
  }

  // [typeId(4Bytes), relationId(8Bytes)]
  def relationTypeKeyToBytes(typeId: Int, relationId: Long): Array[Byte] ={
    val bytes = new Array[Byte](12)
    ByteUtils.setInt(bytes, 0, typeId)
    ByteUtils.setLong(bytes, 4, relationId)
    bytes
  }

  // [typeId(4Bytes), ----]
  def relationTypePrefixToBytes(typeId: Int): Array[Byte] ={
    ByteUtils.intToBytes(typeId)
  }

  // [keyType(1Bytes),id(4Bytes)]
  def nodeLabelKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabel.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // [keyType(1Bytes),--]
  def nodeLabelKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.NodeLabel.id.toByte)
    bytes
  }

  // [keyType(1Bytes),id(4Bytes)]
  def relationTypeKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.RelationType.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // [keyType(1Bytes),--]
  def relationTypeKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.RelationType.id.toByte)
    bytes
  }

  // [keyType(1Bytes),id(4Bytes)]
  def propertyNameKeyToBytes(labelId: Int): Array[Byte] ={
    val bytes = new Array[Byte](5)
    ByteUtils.setByte(bytes, 0, KeyType.PropertyName.id.toByte)
    ByteUtils.setInt(bytes, 1, labelId)
    bytes
  }

  // [keyType(1Bytes),--]
  def propertyNameKeyPrefixToBytes(): Array[Byte] ={
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.PropertyName.id.toByte)
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

  def setDouble(bytes: Array[Byte], index: Int, value: Double): Unit =
    setLong(bytes, index, java.lang.Double.doubleToLongBits(value))

  def getDouble(bytes: Array[Byte], index: Int): Double =
    java.lang.Double.longBitsToDouble(getLong(bytes, index))

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

  def setFloat(bytes: Array[Byte], index: Int, value: Float): Unit =
    setInt(bytes, index, java.lang.Float.floatToIntBits(value))

  def getFloat(bytes: Array[Byte], index: Int): Float =
    java.lang.Float.intBitsToFloat(getInt(bytes, index))

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

  import org.nustaq.serialization.FSTConfiguration
  val conf: FSTConfiguration = FSTConfiguration.createDefaultConfiguration
  def mapToBytes(map: Map[String, Any]): Array[Byte] = {
      conf.asByteArray(map)
    }

  def mapFromBytes(bytes: Array[Byte]): Map[String, Any] = {
//    val bis=new ByteArrayInputStream(bytes)
//    val ois=new ObjectInputStream(bis)
//    ois.readObject.asInstanceOf[Map[String, Any]]
    conf.asObject(bytes).asInstanceOf[Map[String, Any]]
  }


  def longToBytes(num: Long): Array[Byte] = {
    val bytes = new Array[Byte](8)
    ByteUtils.setLong(bytes, 0, num)
    bytes
  }

  def doubleToBytes(num: Double): Array[Byte] = {
    val bytes = new Array[Byte](8)
    ByteUtils.setDouble(bytes, 0, num)
    bytes
  }

  def intToBytes(num: Int): Array[Byte] = {
    val bytes = new Array[Byte](4)
    ByteUtils.setInt(bytes, 0, num)
    bytes
  }

  def floatToBytes(num: Float): Array[Byte] = {
    val bytes = new Array[Byte](4)
    ByteUtils.setFloat(bytes, 0, num)
    bytes
  }

  def shortToBytes(num: Short): Array[Byte] = {
    val bytes = new Array[Byte](2)
    ByteUtils.setShort(bytes, 0, num)
    bytes
  }

  def byteToBytes(num: Byte): Array[Byte] = Array[Byte](num)

  def stringToBytes(str: String, charset: Charset = StandardCharsets.UTF_8): Array[Byte] = {
    str.getBytes(charset)
  }

  def stringFromBytes(bytes: Array[Byte], offset: Int = 0, length:Int = 0,
                      charset: Charset = StandardCharsets.UTF_8): String = {
    if (length > 0) new String(bytes, offset, length, charset)
    else new String(bytes, offset, bytes.length, charset)
  }

  
}
