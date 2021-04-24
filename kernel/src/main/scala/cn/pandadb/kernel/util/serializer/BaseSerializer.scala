package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.blob.api.Blob
import cn.pandadb.kernel.blob.impl.BlobFactory
import cn.pandadb.kernel.util.serializer.BaseSerializer.bytes2Map

import java.io.ByteArrayOutputStream
import java.util.Date
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}

import scala.collection.mutable
import collection.JavaConverters._

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:15 2020/12/17
 * @Modified By: Airzihao at 2021/04/24
 */


object BaseSerializer extends BaseSerializer {

  override val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  def anyArray2Bytes(array: Array[Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    _writeArray(array, byteBuf)
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2AnyArray(bytes: Array[Byte]): Array[_] = {
    val byteBuf = Unpooled.wrappedBuffer(bytes)
    val arr = _readArray(byteBuf)
    byteBuf.release()
    arr
  }

  def intArray2Bytes(array: Array[Int]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    _writeIntArray(array, byteBuf)
    val bytes = exportBytes(byteBuf)
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
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2Map(bytesArr: Array[Byte]): Map[Int, Any] = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArr)
    val map = readMap(byteBuf)
    byteBuf.release()
    map
  }

  def intArrayMap2Bytes(array: Array[Int], map: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    _writeIntArray(array, byteBuf)
    _writeMap(map, byteBuf)
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2IntArrayMap(bytes: Array[Byte]): (Array[Int], Map[Int, Any]) = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytes)
    val intArray: Array[Int] = _readIntArray(byteBuf)
    val map: Map[Int, Any] = readMap(byteBuf)
    byteBuf.release()
    (intArray, map)
  }

  def intSeq2Bytes(seq: Seq[Int]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.buffer()
    byteBuf.writeInt(seq.length)
    seq.foreach(item => byteBuf.writeInt(item))
    val bytes: Array[Byte] = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  // no use?
  def bytes2IntQueue(bytes: Array[Byte]): mutable.Queue[Int] = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytes)
    readIntQueue(byteBuf)
  }

  def bytes2Int(bytes: Array[Byte]): Int = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytes)
    val result: Int = byteBuf.readInt()
    byteBuf.release()
    result
  }

  def bytes2Long(bytes: Array[Byte]): Long = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytes)
    val result: Long = byteBuf.readLong()
    byteBuf.release()
    result
  }

}

trait BaseSerializer {

  // data type:  Map("String"->1, "Int" -> 2, "Long" -> 3, "Double" -> 4, "Float" -> 5, "Boolean" -> 6,
  // "Array[]" -> 7", "blob" -> 8, "date" -> 9)
  val allocator: ByteBufAllocator

  def serialize(longNum: Long): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.buffer()
    byteBuf.writeLong(longNum)
    val bytes: Array[Byte] = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def serialize(intNum: Int): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.buffer()
    byteBuf.writeInt(intNum)
    val bytes: Array[Byte] = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  protected def _writeString(value: String, byteBuf: ByteBuf): Unit = {
    val strInBytes: Array[Byte] = value.getBytes
    val length: Int = strInBytes.length
    byteBuf.writeByte(1)

    def _writeShortString(): Unit = {
      byteBuf.writeShort(length)
      byteBuf.writeBytes(strInBytes)
    }

    def _writeLongString(): Unit = {
      byteBuf.writeShort(-1)
      byteBuf.writeInt(length)
      byteBuf.writeBytes(strInBytes)
    }

    // pow(2,15) = 32768
    if (length < 32767) _writeShortString()
    else _writeLongString()
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

  protected def _writeBoolean(value: Boolean, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(6)
    byteBuf.writeBoolean(value)
  }

  protected def _writeBlob(value: Blob, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(8)
    val blobInBytes = value.toBytes()
    byteBuf.writeInt(blobInBytes.length)
    byteBuf.writeBytes(blobInBytes)
  }

  protected def _writeDate(value: Date, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(9)
    byteBuf.writeLong(value.getTime)
  }

  protected def _writeKV(keyId: Int, value: Any, byteBuf: ByteBuf): Any = {
    byteBuf.writeShort(keyId)
    value match {
      case s: Boolean => _writeBoolean(value.asInstanceOf[Boolean], byteBuf)
      case s: Double => _writeDouble(value.asInstanceOf[Double], byteBuf)
      case s: Float => _writeFloat(value.asInstanceOf[Float], byteBuf)
      case s: Int => _writeInt(value.asInstanceOf[Int], byteBuf)
      case s: Long => _writeLong(value.asInstanceOf[Long], byteBuf)
      case s: Array[_] => _writeArray(value.asInstanceOf[Array[_]], byteBuf)
      case s: Blob => _writeBlob(value.asInstanceOf[Blob], byteBuf)
      case s: Date => _writeDate(value.asInstanceOf[Date], byteBuf)
      case _ => {
//        println(value.getClass)
        _writeString(value.asInstanceOf[String], byteBuf)
      }
    }
  }

  protected def _writeArray(array: Array[_], byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(7)
    val len: Int = array.length
    byteBuf.writeInt(len)

    array match {
      case s: Array[String] => {
        byteBuf.writeByte(1)
        array.asInstanceOf[Array[String]].foreach(item => _writeString(item, byteBuf))
      }
      case s: Array[Int] => {
        byteBuf.writeByte(2)
        array.asInstanceOf[Array[Int]].foreach(item => byteBuf.writeInt(item))
      }
      case s: Array[Long] => {
        byteBuf.writeByte(3)
        array.asInstanceOf[Array[Long]].foreach(item => byteBuf.writeLong(item))
      }
      case s: Array[Double] => {
        byteBuf.writeByte(4)
        array.asInstanceOf[Array[Double]].foreach(item => byteBuf.writeDouble(item))
      }
      case s: Array[Float] => {
        byteBuf.writeByte(5)
        array.asInstanceOf[Array[Float]].foreach(item => byteBuf.writeFloat(item))
      }
      case s: Array[Boolean] => {
        byteBuf.writeByte(6)
        array.asInstanceOf[Array[Boolean]].foreach(item => byteBuf.writeBoolean(item))
      }
    }
  }

  protected def _readArray[T](byteBuf: ByteBuf): Array[_] = {
    val len = byteBuf.readInt()
    val typeFlag = byteBuf.readByte().toInt
    typeFlag match {
      case 1 => new Array[String](len).map(item => {
        val typeFlag: Int = byteBuf.readByte().toInt // drop the flag
        _readString(byteBuf)})
      case 2 => new Array[Int](len).map(item => byteBuf.readInt())
      case 3 => new Array[Long](len).map(item => byteBuf.readLong())
      case 4 => new Array[Double](len).map(item => byteBuf.readDouble())
      case 5 => new Array[Float](len).map(item => byteBuf.readFloat())
      case 6 => new Array[Boolean](len).map(item => byteBuf.readBoolean())
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

  protected def _readAny(byteBuf: ByteBuf): Any = {
    val typeId: Int = byteBuf.readByte().toInt
    val value = typeId match {
      case 1 => _readString(byteBuf)
      case 2 => byteBuf.readInt()
      case 3 => byteBuf.readLong()
      case 4 => byteBuf.readDouble()
      case 5 => byteBuf.readFloat()
      case 6 => byteBuf.readBoolean()
      case 9 => _readDate(byteBuf)
      //      case _ => _readString(byteBuf)
    }
    value
  }

  protected def _readString(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readShort().toInt
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()

    //short
    if (len < 32768 && len > 0) {
      byteBuf.readBytes(bos, len)
    }
    //long
    else {
      val trueLen: Int = byteBuf.readInt()
      byteBuf.readBytes(bos, trueLen)
    }
    bos.toString
  }



  protected def _readBlob(byteBuf: ByteBuf): Blob = {
    val len: Int = byteBuf.readInt()
    val bytesArray : Array[Byte] = new Array[Byte](len)
    byteBuf.readBytes(bytesArray)
    BlobFactory.fromBytes(bytesArray)
  }

  protected def _readDate(byteBuf: ByteBuf): Date = {
    val time = byteBuf.readLong()
    new Date(time)
  }

  protected def _readIntArray(byteBuf: ByteBuf): Array[Int] = {
    val len = byteBuf.readByte().toInt
    new Array[Int](len).map(item => byteBuf.readInt())
  }

  def readMap(byteBuf: ByteBuf): Map[Int, Any] = {
    val propNum: Int = byteBuf.readByte().toInt
    val propsMap: Map[Int, Any] = new Array[Int](propNum).map(item => {
      val propId: Int = byteBuf.readShort().toInt
      val propType: Int = byteBuf.readByte().toInt
      val propValue = propType match {
        case 1 => _readString(byteBuf)
        case 2 => byteBuf.readInt()
        case 3 => byteBuf.readLong()
        case 4 => byteBuf.readDouble()
        case 5 => byteBuf.readFloat()
        case 6 => byteBuf.readBoolean()
        case 7 => _readArray(byteBuf)
        case 8 => _readBlob(byteBuf)
        case 9 => _readDate(byteBuf)
        case _ => _readString(byteBuf)
      }
      propId -> propValue
    }).toMap
    propsMap
  }

  def readIntQueue(byteBuf: ByteBuf): mutable.Queue[Int] = {
    val length = byteBuf.readInt()
    val queue: mutable.Queue[Int] = new mutable.Queue[Int]()
    for (i <- 1 to length) queue.enqueue(byteBuf.readInt())
    queue
  }

  def exportBytes(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    dst
  }
}