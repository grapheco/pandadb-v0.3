package cn.pandadb.kernel.kv

import java.io.ByteArrayOutputStream

import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}


trait Node {

  def getId(): Long;

  def getProperty(key: String): Any;

  def getAllProperties(): Map[String, Any];

  def getLabels(): Array[String];

  def setProperty(key:String, value: Any): Unit = ???;

  def removeProperty(key:String): Any = ???;

  def addLabel(label: String): Unit = ???;

  def removeLabel(label: String): Unit = ???;
}

class SerializedNode(id:Long,
                     override val labelIdsBytes: Array[Byte],
                     override val propertiesBytes: Array[Byte],
                     nodeStoreSpi: NodeStoreSPI)
  extends LazyNode(id, nodeStoreSpi) {
}

class LazyNode(id: Long, nodeStoreSpi: NodeStoreSPI) extends Node {

  lazy val labelIdsBytes: Array[Byte] = nodeStoreSpi.getNodeLabelIdsBytes(id)
  lazy val propertiesBytes: Array[Byte] = nodeStoreSpi.getNodePropertiesBytes(id)

  lazy val labelIds: Array[Int] = Serializer.bytes2IntArr(labelIdsBytes)
  lazy val propertyMap: Map[Int, Any] = Serializer.bytes2Map(propertiesBytes)

  def getLabelIds(): Array[Int] = {
    labelIds
  }

  def getPropertyByKeyId(keyId: Int): Option[Any] = {
    propertyMap.get(keyId)
  }

  override def getId(): Long = id

  override def getLabels(): Array[String] = {
    labelIds.map(id => nodeStoreSpi.getLabelName(id))
  }

  override def getProperty(key: String): Any = {
    propertyMap.get(nodeStoreSpi.getPropertyKeyId(key)).get
  }

  override def getAllProperties(): Map[String, Any] = {
    propertyMap.map(kv => (nodeStoreSpi.getPropertyKeyName(kv._1), kv._2))
  }

}

class NodeWithProperty(id:Long,
                     override val propertiesBytes: Array[Byte],
                     nodeStoreSpi: NodeStoreSPI)
  extends LazyNode(id, nodeStoreSpi) {
}

class NodeWithLabels(id:Long,
                       override val labelIdsBytes: Array[Byte],
                       nodeStoreSpi: NodeStoreSPI)
  extends LazyNode(id, nodeStoreSpi) {
}


trait NodeStoreSPI {
  def allLabels(): Array[String];
  def allLabelIds(): Array[Int];
  def getLabelName(labelId: Int): String;
  def getLabelId(labelName: String): Int;
  def getPropertyKeyName(keyId: Int): String;
  def getPropertyKeyId(keyName: String): Int;


  def getNodeLabelIdsBytes(nodeId: Long): Array[Byte];
  def getNodePropertiesBytes(nodeId: Long): Array[Byte];

  def nodeAddLabel(nodeId: Long, labelId:Long): Unit;
  def nodeRemoveLabel(nodeId: Long, labelId:Long): Unit;

  def nodeSetProperty(nodeId: Long, propertyKeyId: Long, propertyValue: Any): Unit;
  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Long): Any;
}


object Serializer {

  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT
  // [byte:len][arr(0)][arr(1)]...
  def intArr2Bytes(arr: Array[Int]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    val len = arr.length
    byteBuf.writeByte(len)
    arr.foreach(item => byteBuf.writeInt(item))
    val bos = new ByteArrayOutputStream()
    val offset = byteBuf.writerIndex()
    byteBuf.readBytes(bos, offset)
    byteBuf.release()
    bos.toByteArray
  }

  def bytes2IntArr(bytesArr: Array[Byte]): Array[Int] = {
    val byteBuf: ByteBuf = allocator.buffer()
    byteBuf.writeBytes(bytesArr)
    val len = byteBuf.readByte().toInt
    val arr = new Array[Int](len).map(item => byteBuf.readInt())
    byteBuf.release()
    arr
  }

  // [size][key1][type1][len(if needed)][value1]
  def map2Bytes(map: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.buffer()
    val size: Int = map.size
    byteBuf.writeByte(size)
    map.foreach(kv => _writeKV(kv._1, kv._2, byteBuf))
    val offset = byteBuf.writerIndex()
    val bos = new ByteArrayOutputStream()
    byteBuf.readBytes(bos, offset)
    byteBuf.release()
    bos.toByteArray
  }

  def bytes2Map(bytesArr: Array[Byte]): Map[Int, Any] = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArr)
    //    byteBuf.writeBytes(bytesArr)
    val map = _bytes2Map(byteBuf)
    byteBuf.release()
    map
  }

  protected def _bytes2Map(byteBuf: ByteBuf): Map[Int, Any] = {
    val propNum: Int = byteBuf.readByte().toInt
    val propsMap: Map[Int, Any] = new Array[Int](propNum).map(item => {
      val propId: Int = byteBuf.readByte().toInt
      val propType: Int = byteBuf.readByte().toInt
      val propValue = propType match {
        case 1 => _readString(byteBuf)
        case 2 => byteBuf.readInt()
        case 3 => byteBuf.readLong()
        case 4 => byteBuf.readDouble()
        case 5 => byteBuf.readFloat()
        case 6 => byteBuf.readBoolean()
        case _ => _readString(byteBuf)
      }
      propId -> propValue
    }).toMap
    propsMap
  }

  protected def _writeString(value: String, byteBuf: ByteBuf): Unit = {
    val strInBytes: Array[Byte] = value.getBytes
    val length: Int = strInBytes.length
    // write type
    byteBuf.writeByte(1)
    byteBuf.writeByte(length)
    byteBuf.writeBytes(strInBytes)
  }

  protected def _writeInt(value: Int, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(2)
    byteBuf.writeInt(value)
  }

  protected def _writeLong(value: Long, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(3)
    byteBuf.writeLong(value)
  }

  protected def _writeDouble(value: Double, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(4)
    byteBuf.writeDouble(value)
  }

  protected def _writeFloat(value: Float, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(5)
    byteBuf.writeFloat(value)
  }

  protected def _writeBoolean(value: Boolean, byteBuf: ByteBuf) = {
    byteBuf.writeByte(6)
    byteBuf.writeBoolean(value)
  }

  protected def _readString(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readByte().toInt
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    byteBuf.readBytes(bos, len)
    bos.toString
  }

  protected def _writeKV(keyId: Int, value: Any, byteBuf: ByteBuf) = {
    byteBuf.writeByte(keyId)
    value match {
      case s: Boolean => _writeBoolean(value.asInstanceOf[Boolean], byteBuf)
      case s: Double => _writeDouble(value.asInstanceOf[Double], byteBuf)
      case s: Float => _writeFloat(value.asInstanceOf[Float], byteBuf)
      case s: Int => _writeInt(value.asInstanceOf[Int], byteBuf)
      case s: Long => _writeLong(value.asInstanceOf[Long], byteBuf)
      case _ => _writeString(value.asInstanceOf[String], byteBuf)
    }
  }

}