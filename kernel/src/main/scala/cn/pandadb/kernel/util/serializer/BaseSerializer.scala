package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.blob.api.Blob
import cn.pandadb.kernel.blob.impl.BlobFactory
import cn.pandadb.kernel.util.serializer.BaseSerializer.bytes2Map
import java.io.ByteArrayOutputStream
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.Date

import cn.pandadb.kernel.kv.value.LynxDateTimeUtil
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}
import org.grapheco.lynx.LynxDateTime

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
  object DataType extends Enumeration {
    type DataType = Value

    val STRING = Value(1, "String")
    val INT = Value(2, "Int")
    val LONG = Value(3, "Long")
    val DOUBLE = Value(4, "Double")
    val FLOAT = Value(5, "Float")
    val BOOLEAN = Value(6, "Boolean")
    val ARRAY = Value(7, "Array")
    val BLOB = Value(8, "Blob")
    val DATE = Value(9, "Date")
    val DATE_TIME = Value(10, "DateTime")
    val LOCAL_DATE_TIME = Value(11, "LocalDateTime")
    val LOCAL_TIME = Value(12, "LocalTime")
    val TIME = Value(13, "Time")
  }

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
    byteBuf.writeByte(DataType.STRING.id.toByte)

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
    byteBuf.writeByte(DataType.INT.id.toByte)
    byteBuf.writeInt(value)
  }

  protected def _writeLong(value: Long, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.LONG.id.toByte)
    byteBuf.writeLong(value)
  }

  protected def _writeDouble(value: Double, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.DOUBLE.id.toByte)
    byteBuf.writeDouble(value)
  }

  protected def _writeFloat(value: Float, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.FLOAT.id.toByte)
    byteBuf.writeFloat(value)
  }

  protected def _writeBoolean(value: Boolean, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(DataType.BOOLEAN.id.toByte)
    byteBuf.writeBoolean(value)
  }

  protected def _writeBlob(value: Blob, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(DataType.BLOB.id.toByte)
    val blobInBytes = value.toBytes()
    byteBuf.writeInt(blobInBytes.length)
    byteBuf.writeBytes(blobInBytes)
  }

  protected def _writeDate(value: LocalDate, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.DATE.id.toByte)
    byteBuf.writeLong(value.toEpochDay)
  }

  protected def _readDate(byteBuf: ByteBuf): LocalDate = {
    val epochDay = byteBuf.readLong()
    LocalDate.ofEpochDay(epochDay)
  }

  protected def _writeDateTime(value: ZonedDateTime, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.DATE_TIME.id.toByte)

    val epochSecondUTC: Long = value.toEpochSecond
    val nano: Int = value.getNano
    val zoneId: String = value.getZone.getId
    byteBuf.writeLong(epochSecondUTC)
    byteBuf.writeInt(nano)
    _writeString(zoneId, byteBuf)
  }

  protected def _readDateTime(byteBuf: ByteBuf): ZonedDateTime = {
    val epochSecondUTC: Long = byteBuf.readLong()
    val nano: Int = byteBuf.readInt()
    val zoneStoreType = byteBuf.readByte()
    val zone: String = _readString(byteBuf)
    val zoneId: ZoneId = LynxDateTimeUtil.parseZone(zone)
    ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecondUTC, nano), zoneId)
  }

  protected def _writeLocalDateTime(value: LocalDateTime, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.LOCAL_DATE_TIME.id.toByte)

    val epochSecond: Long = value.toEpochSecond(ZoneOffset.UTC)
    val nano: Int = value.getNano
    byteBuf.writeLong(epochSecond)
    byteBuf.writeInt(nano)
  }

  protected def _readLocalDateTime(byteBuf: ByteBuf): LocalDateTime = {
    val epochSecond: Long = byteBuf.readLong()
    val nano: Int = byteBuf.readInt()
    LocalDateTime.ofEpochSecond(epochSecond, nano, ZoneOffset.UTC)
  }

  protected def _writeTime(value: OffsetTime, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.TIME.id.toByte)

    val localNanosOfDay: Long = value.toLocalTime.toNanoOfDay
    val offsetSeconds: Int = value.getOffset.getTotalSeconds
    byteBuf.writeLong(localNanosOfDay)
    byteBuf.writeInt(offsetSeconds)
  }

  protected def _readTime(byteBuf: ByteBuf): OffsetTime = {
    val localNanosOfDay: Long = byteBuf.readLong()
    val offsetSeconds: Int = byteBuf.readInt()
    val zoneOffset: ZoneOffset = ZoneOffset.ofTotalSeconds(offsetSeconds)
    OffsetTime.of(LocalTime.ofNanoOfDay(localNanosOfDay), zoneOffset)
  }

  protected def _writeLocalTime(value: LocalTime, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.LOCAL_TIME.id.toByte)

    val localNanosOfDay: Long = value.toNanoOfDay
    byteBuf.writeLong(localNanosOfDay)
  }

  protected def _readLocalTime(byteBuf: ByteBuf): LocalTime = {
    val localNanosOfDay: Long = byteBuf.readLong()
    LocalTime.ofNanoOfDay(localNanosOfDay)
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
      case s: LocalDate => _writeDate(s, byteBuf) // date()
      case s: LocalDateTime => _writeLocalDateTime(s, byteBuf) // localdatetime()
      case s: LocalTime => _writeLocalTime(s, byteBuf) // localtime()
      case s: ZonedDateTime => _writeDateTime(s, byteBuf) // datetime()
      case s: OffsetTime => _writeTime(s, byteBuf) // time()
      case _ => {
        _writeString(value.asInstanceOf[String], byteBuf)
      }
    }
  }

  protected def _writeArray(array: Array[_], byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(DataType.ARRAY.id.toByte)
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

//  protected def _readAny(byteBuf: ByteBuf): Any = {
//    val typeId: Int = byteBuf.readByte().toInt
//    val value = typeId match {
//      case 1 => _readString(byteBuf)
//      case 2 => byteBuf.readInt()
//      case 3 => byteBuf.readLong()
//      case 4 => byteBuf.readDouble()
//      case 5 => byteBuf.readFloat()
//      case 6 => byteBuf.readBoolean()
//      case 9 => _readDate(byteBuf)
//      //      case _ => _readString(byteBuf)
//    }
//    value
//  }

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

//  protected def _readDate(byteBuf: ByteBuf): Date = {
//    val time = byteBuf.readLong()
//    new Date(time)
//  }

  protected def _readIntArray(byteBuf: ByteBuf): Array[Int] = {
    val len = byteBuf.readByte().toInt
    new Array[Int](len).map(item => byteBuf.readInt())
  }

  def readMap(byteBuf: ByteBuf): Map[Int, Any] = {
    val propNum: Int = byteBuf.readByte().toInt
    val propsMap: Map[Int, Any] = new Array[Int](propNum).map(item => {
      val propId: Int = byteBuf.readShort().toInt
      val propType: Int = byteBuf.readByte().toInt
      val propValue = DataType(propType) match {
        case DataType.STRING => _readString(byteBuf)
        case DataType.INT => byteBuf.readInt()
        case DataType.LONG => byteBuf.readLong()
        case DataType.DOUBLE => byteBuf.readDouble()
        case DataType.FLOAT => byteBuf.readFloat()
        case DataType.BOOLEAN => byteBuf.readBoolean()
        case DataType.ARRAY => _readArray(byteBuf)
        case DataType.BLOB => _readBlob(byteBuf)
        case DataType.DATE => _readDate(byteBuf)
        case DataType.DATE_TIME => _readDateTime(byteBuf)
        case DataType.TIME => _readTime(byteBuf)
        case DataType.LOCAL_DATE_TIME => _readLocalDateTime(byteBuf)
        case DataType.LOCAL_TIME => _readLocalTime(byteBuf)
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