package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.blob.api.Blob
import cn.pandadb.kernel.blob.impl.BlobFactory
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}

import java.io.ByteArrayOutputStream
import java.time._
import scala.collection.mutable

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:15 2020/12/17
 * @Modified By: Airzihao at 2021/04/24
 */

object BaseSerializer extends BaseSerializer {

  def array2Bytes(array: Array[_]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    ArraySerializer.writeArray(array, byteBuf)
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2Array(bytes: Array[Byte]): Array[_] = {
    val byteBuf = Unpooled.wrappedBuffer(bytes)
    val arr = ArraySerializer.readArray(byteBuf)
    byteBuf.release()
    arr
  }

  // [size][key1][type1][len(if needed)][value1]
  def map2Bytes(map: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.buffer()
    MapSerializer.writeMap(map, byteBuf)
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def bytes2Map(bytesArr: Array[Byte]): Map[Int, Any] = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArr)
    val map = MapSerializer.readMap(byteBuf)
    byteBuf.release()
    map
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

  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

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
    byteBuf.writeByte(SerialzerDataType.STRING.id.toByte)
    val strInBytes: Array[Byte] = value.getBytes
    val length: Int = strInBytes.length

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
  protected def _readString(byteBuf: ByteBuf): String = {
    val len: Int = byteBuf.readShort().toInt
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream()

    //short
    if (len < 32768 && len >= 0) {
      byteBuf.readBytes(bos, len)
    }
    //long
    else {
      val trueLen: Int = byteBuf.readInt()
      byteBuf.readBytes(bos, trueLen)
    }
    bos.toString
  }

  protected def _writeInt(value: Int, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.INT.id.toByte)
    byteBuf.writeInt(value)
  }

  protected def _writeLong(value: Long, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.LONG.id.toByte)
    byteBuf.writeLong(value)
  }

  protected def _writeDouble(value: Double, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.DOUBLE.id.toByte)
    byteBuf.writeDouble(value)
  }

  protected def _writeFloat(value: Float, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.FLOAT.id.toByte)
    byteBuf.writeFloat(value)
  }

  protected def _writeBoolean(value: Boolean, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(SerialzerDataType.BOOLEAN.id.toByte)
    byteBuf.writeBoolean(value)
  }

  protected def _writeBlob(value: Blob, byteBuf: ByteBuf): ByteBuf = {
    byteBuf.writeByte(SerialzerDataType.BLOB.id.toByte)
    val blobInBytes = value.toBytes()
    byteBuf.writeInt(blobInBytes.length)
    byteBuf.writeBytes(blobInBytes)
  }

  protected def _writeAny(value: Any, byteBuf: ByteBuf): Unit = {
    value match {
      case s: String => _writeString(s, byteBuf)
      case s: Int => _writeInt(s, byteBuf)
      case s: Long => _writeLong(s, byteBuf)
      case s: Double => _writeDouble(s, byteBuf)
      case s: Float => _writeFloat(s, byteBuf)
      case s: Boolean => _writeBoolean(s, byteBuf)
    }
  }

  protected def _readAny(byteBuf: ByteBuf): Any = {
    val typeFlag = byteBuf.readByte().toInt
    SerialzerDataType(typeFlag) match {
      case SerialzerDataType.STRING => _readString(byteBuf)
      case SerialzerDataType.INT => byteBuf.readInt()
      case SerialzerDataType.LONG => byteBuf.readLong()
      case SerialzerDataType.DOUBLE => byteBuf.readDouble()
      case SerialzerDataType.FLOAT => byteBuf.readFloat()
      case SerialzerDataType.BOOLEAN => byteBuf.readBoolean()
    }
  }

  def readIntQueue(byteBuf: ByteBuf): mutable.Queue[Int] = {
    val length = byteBuf.readInt()
    val queue: mutable.Queue[Int] = new mutable.Queue[Int]()
    for (i <- 1 to length) queue.enqueue(byteBuf.readInt())
    queue
  }

  protected def _writeDate(value: LocalDate, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.DATE.id.toByte)
    byteBuf.writeLong(value.toEpochDay)
  }

  protected def _readDate(byteBuf: ByteBuf): LocalDate = {
    val epochDay = byteBuf.readLong()
    LocalDate.ofEpochDay(epochDay)
  }

  protected def _writeDateTime(value: ZonedDateTime, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.DATE_TIME.id.toByte)

    val epochSecondUTC: Long = value.toEpochSecond
    val nano: Int = value.getNano
    val zoneId: String = value.getZone.getId
    byteBuf.writeLong(epochSecondUTC)
    byteBuf.writeInt(nano)
    _writeString(zoneId, byteBuf)
  }

//  protected def _readDateTime(byteBuf: ByteBuf): ZonedDateTime = {
//    val epochSecondUTC: Long = byteBuf.readLong()
//    val nano: Int = byteBuf.readInt()
//    val zoneStoreType = byteBuf.readByte()
//    val zone: String = _readString(byteBuf)
//    val zoneId: ZoneId = LynxDateTimeUtil.parseZone(zone)
//    ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecondUTC, nano), zoneId)
//  }

  protected def _writeLocalDateTime(value: LocalDateTime, byteBuf: ByteBuf): Unit = {
    byteBuf.writeByte(SerialzerDataType.LOCAL_DATE_TIME.id.toByte)

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
    byteBuf.writeByte(SerialzerDataType.TIME.id.toByte)

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
    byteBuf.writeByte(SerialzerDataType.LOCAL_TIME.id.toByte)

    val localNanosOfDay: Long = value.toNanoOfDay
    byteBuf.writeLong(localNanosOfDay)
  }

  protected def _readLocalTime(byteBuf: ByteBuf): LocalTime = {
    val localNanosOfDay: Long = byteBuf.readLong()
    LocalTime.ofNanoOfDay(localNanosOfDay)
  }

  protected def _readBlob(byteBuf: ByteBuf): Blob = {
    val len: Int = byteBuf.readInt()
    val bytesArray : Array[Byte] = new Array[Byte](len)
    byteBuf.readBytes(bytesArray)
    BlobFactory.fromBytes(bytesArray)
  }

  def exportBytes(byteBuf: ByteBuf): Array[Byte] = {
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    dst
  }

}