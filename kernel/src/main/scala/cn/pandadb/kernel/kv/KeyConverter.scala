package cn.pandadb.kernel.kv

import java.nio.charset.{Charset, StandardCharsets}

import org.tikv.common.types.{Charset=>TiKVCharset}

class IllegalKeyException(s: String) extends RuntimeException(s) {
}

object KeyConverter {
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

    val NodeIdGenerator = Value(13)     // [keyType(1Byte)] -> MaxId(Long)
    val RelationIdGenerator = Value(14)     // [keyType(1Byte)] -> MaxId(Long)
    val IndexIdGenerator = Value(15)     // [keyType(1Byte)] -> MaxId(Long)

  }

  type NodeId     = Long
  type RelationId = Long
  type LabelId    = Int
  type TypeId     = Int
  type PropertyId = Int
  type IndexId    = Int
  type Value      = Array[Byte]

  val NODE_ID_SIZE     = 8
  val RELATION_ID_SIZE = 8
  val LABEL_ID_SIZE    = 4
  val TYPE_ID_SIZE     = 4
  val PROPERTY_ID_SIZE = 4
  val INDEX_ID_SIZE    = 4
  val IS_FULLTEXT_SIZE = 1


  //
  def nodeMaxIdKey: Array[Byte] = "nodeMaxId".getBytes(TiKVCharset.CharsetUTF8)
  def relationMaxIdKey: Array[Byte] = "relationMaxId".getBytes(TiKVCharset.CharsetUTF8)


  /**
   * ╔══════════════════╗
   * ║        key       ║
   * ╠═════════╦════════╣
   * ║ labelId ║ nodeId ║
   * ╚═════════╩════════╝
   */
  def toNodeKey(labelId: LabelId, nodeId: NodeId): Array[Byte] = {
    val bytes = new Array[Byte](LABEL_ID_SIZE + NODE_ID_SIZE)
    ByteUtils.setInt(bytes, 0, labelId)
    ByteUtils.setLong(bytes, LABEL_ID_SIZE, nodeId)
    bytes
  }

  def toNodeKey(labelId: LabelId): Array[Byte] = ByteUtils.intToBytes(labelId)

  def getLabelIdInNodeKey(array: Array[Byte]):LabelId = ByteUtils.getInt(array, 0)

  def getNodeIdInNodeKey(array: Array[Byte]):NodeId = ByteUtils.getLong(array, LABEL_ID_SIZE)

  /**
   * ╔═══════════════════╗
   * ║        key        ║
   * ╠═════════╦═════════╣
   * ║ nodeId  ║ labelId ║
   * ╚═════════╩═════════╝
   */
  def toNodeLabelKey(nodeId: NodeId, labelId: LabelId): Array[Byte] = {
    val bytes = new Array[Byte](LABEL_ID_SIZE + NODE_ID_SIZE)
    ByteUtils.setLong(bytes, 0, nodeId)
    ByteUtils.setInt(bytes, NODE_ID_SIZE, labelId)
    bytes
  }

  // [nodeId(8Bytes), ----]
  def toNodeLabelKey(nodeId: NodeId): Array[Byte] = ByteUtils.longToBytes(nodeId)

  def getNodeIdInNodeLabelKey(array: Array[Byte]):NodeId = ByteUtils.getLong(array, 0)

  def getLabelIdInNodeLabelKey(array: Array[Byte]):LabelId = ByteUtils.getInt(array, NODE_ID_SIZE)


  /**
   * ╔═════════════════════════════╗
   * ║              key            ║
   * ╠═══════╦═══════╦═════════════╣
   * ║ label ║ props ║   fullText  ║
   * ╚═══════╩═══════╩═════════════╝
   */
  def toIndexMetaKey(labelId: LabelId, props:Array[PropertyId], isFullText: Boolean): Array[Byte] = {
    val bytes = new Array[Byte](LABEL_ID_SIZE + PROPERTY_ID_SIZE*props.length + IS_FULLTEXT_SIZE)
    ByteUtils.setInt(bytes, 0, labelId)
    var index = LABEL_ID_SIZE
    props.foreach{
      p=>ByteUtils.setInt(bytes,index,p)
        index += PROPERTY_ID_SIZE
    }
    ByteUtils.setBoolean(bytes, bytes.length - IS_FULLTEXT_SIZE, isFullText)
    bytes
  }

  def getIndexMetaFromKey(keyBytes: Array[Byte]): (LabelId, Array[PropertyId], Boolean) = {
    if(keyBytes.length < LABEL_ID_SIZE + PROPERTY_ID_SIZE + IS_FULLTEXT_SIZE) return null
    (
      ByteUtils.getInt(keyBytes, 0),
      (0 until (keyBytes.length - LABEL_ID_SIZE - IS_FULLTEXT_SIZE)/PROPERTY_ID_SIZE).
        toArray.map(i => ByteUtils.getInt(keyBytes, LABEL_ID_SIZE+PROPERTY_ID_SIZE*i)),
      ByteUtils.getBoolean(keyBytes, keyBytes.length - IS_FULLTEXT_SIZE)
    )
  }

  /**
   * ╔══════════════════════════════════════════╗
   * ║                   key                    ║
   * ╠═════════╦══════════╦══════════╦══════════╣
   * ║ indexId ║ typeCode ║  value   ║  nodeId  ║
   * ╚═════════╩══════════╩══════════╩══════════╝
   */
  def toIndexKey(indexId: IndexId, typeCode: Byte, value: Array[Byte], nodeId: NodeId): Array[Byte] = {
    val bytes = new Array[Byte](INDEX_ID_SIZE + NODE_ID_SIZE + 1 + value.length)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, INDEX_ID_SIZE, typeCode)
    for (i <- value.indices)
      bytes(INDEX_ID_SIZE+1+i) = value(i)
    ByteUtils.setLong(bytes, bytes.length - NODE_ID_SIZE, nodeId)
    bytes
  }

  def toIndexKey(indexId:IndexId, typeCode:Byte, value: Array[Byte]): Array[Byte] = {
    val bytes = new Array[Byte](INDEX_ID_SIZE + 1 + value.length)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, INDEX_ID_SIZE, typeCode)
    for (i <- value.indices)
      bytes(INDEX_ID_SIZE+1+i) = value(i)
    bytes
  }

  def toIndexKey(indexId:IndexId, typeCode:Byte): Array[Byte] = {
    val bytes = new Array[Byte](INDEX_ID_SIZE + 1)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, INDEX_ID_SIZE, typeCode)
    bytes
  }

  /**
   * ╔═════════════╗
   * ║     key     ║
   * ╠═════════════╣
   * ║ relationId  ║
   * ╚═════════════╝
   */
  def toRelationKey(relationId: RelationId): Array[Byte] = ByteUtils.longToBytes(relationId)

  /**
   * ╔═════════════════════╗
   * ║         key         ║
   * ╠════════╦════════════╣
   * ║ typeId ║ relationId ║
   * ╚════════╩════════════╝
   */
  def toRelationTypeKey(typeId: TypeId, relationId: RelationId): Array[Byte] = {
    val bytes = new Array[Byte](TYPE_ID_SIZE+RELATION_ID_SIZE)
    ByteUtils.setInt(bytes, 0, typeId)
    ByteUtils.setLong(bytes, TYPE_ID_SIZE, relationId)
    bytes
  }

  def toRelationTypeKey(typeId: TypeId): Array[Byte] = ByteUtils.intToBytes(typeId)

  // TODO change blow

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

  // [keyType(1Bytes)]
  def nodeIdGeneratorKeyToBytes(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.NodeIdGenerator.id.toByte)
    bytes
  }
  // [keyType(1Bytes)]
  def relationIdGeneratorKeyToBytes(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.RelationIdGenerator.id.toByte)
    bytes
  }
  // [KeyType(1Bytes)]
  def indexIdGeneratorKeyToBytes(): Array[Byte] = {
    val bytes = new Array[Byte](1)
    ByteUtils.setByte(bytes, 0, KeyType.IndexIdGenerator.id.toByte)
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

  def setBoolean(bytes: Array[Byte], index: Int, value: Boolean): Unit = {
    bytes(index) = if(value) 1.toByte else 0.toByte
  }

  def getBoolean(bytes: Array[Byte], index: Int): Boolean = {
    bytes(index) == 1.toByte
  }

  def toBytes(value: Any): Array[Byte] = {
    val bytes:Array[Byte] = value match {
      case value: Boolean => if(value) Array[Byte](1) else Array[Byte](0)
      case value: Byte => Array[Byte](value)
      case value: Short => new Array[Byte](2)
      case value: Int => new Array[Byte](4)
      case value: Long => new Array[Byte](8)
      case value: Float => new Array[Byte](4)
      case value: Double =>  new Array[Byte](8)
      case value: String =>  value.getBytes
      case _ => Array.emptyByteArray
    }
    value match {
      case value: Short => ByteUtils.setShort(bytes, 0, value)
      case value: Int => ByteUtils.setInt(bytes, 0, value)
      case value: Long => ByteUtils.setLong(bytes, 0, value)
      case value: Float => ByteUtils.setFloat(bytes, 0, value)
      case value: Double =>  ByteUtils.setDouble(bytes, 0, value)
    }
    bytes
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

  def booleanToBytes(b: Boolean): Array[Byte] = Array[Byte](if(b) 1.toByte else 0.toByte)

  def stringToBytes(str: String, charset: Charset = StandardCharsets.UTF_8): Array[Byte] = {
    str.getBytes(charset)
  }

  def stringFromBytes(bytes: Array[Byte], offset: Int = 0, length:Int = 0,
                      charset: Charset = StandardCharsets.UTF_8): String = {
    if (length > 0) new String(bytes, offset, length, charset)
    else new String(bytes, offset, bytes.length, charset)
  }

  
}
