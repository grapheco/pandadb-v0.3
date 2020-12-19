package cn.pandadb.kernel.util.serializer

import java.io.ByteArrayOutputStream

import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:15 2020/12/17
 * @Modified By:
 */

object BaseSerializer extends BaseSerializer {

  override val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT
  def intArray2Bytes(array: Array[Int]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    _writeIntArray(array, byteBuf)
    val bytes = _exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2IntArray(bytesArr: Array[Byte]): Array[Int] = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArr)
    val array = _readIntArray(byteBuf)
    byteBuf.release()
    array
  }

  // [size][key1][type1][len(if needed)][value1]
  def map2Bytes(map: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.buffer()
    _writeMap(map, byteBuf)
    val bytes = _exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2Map(bytesArr: Array[Byte]): Map[Int, Any] = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArr)
    val map = _readMap(byteBuf)
    byteBuf.release()
    map
  }

  def intArrayMap2Bytes(array: Array[Int], map: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    _writeIntArray(array, byteBuf)
    _writeMap(map, byteBuf)
    val bytes = _exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2IntArrayMap(bytes: Array[Byte]): (Array[Int], Map[Int, Any]) = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytes)
    val intArray: Array[Int] = _readIntArray(byteBuf)
    val map: Map[Int, Any] = _readMap(byteBuf)
    byteBuf.release()
    (intArray, map)
  }
}

trait BaseSerializer {


  // data type:  Map("String"->1, "Int" -> 2, "Long" -> 3, "Double" -> 4, "Float" -> 5, "Boolean" -> 6)
  val allocator: ByteBufAllocator

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

  // [byte:len][arr(0)][arr(1)]...
  protected def _writeIntArray(array: Array[Int], byteBuf: ByteBuf): Unit = {
    val len = array.length
    byteBuf.writeByte(len)
    array.foreach(item => byteBuf.writeInt(item))
  }

  protected def _writeMap(map: Map[Int, Any], byteBuf: ByteBuf): Unit = {
    val size: Int = map.size
    byteBuf.writeByte(size)
    map.foreach(kv => _writeKV(kv._1, kv._2, byteBuf))
  }

  protected def _readString(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readByte().toInt
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
    byteBuf.readBytes(bos, len)
    bos.toString
  }
  protected def _readIntArray(byteBuf: ByteBuf): Array[Int] = {
    val len = byteBuf.readByte().toInt
    new Array[Int](len).map(item => byteBuf.readInt())
  }
  protected def _readMap(byteBuf: ByteBuf): Map[Int, Any] = {
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
  protected def _exportBytes(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    dst
  }

}
