package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.blob.api.Blob
import io.netty.buffer.ByteBuf

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 18:58 2021/4/25
 * @Modified By:
 */
object MapSerializer extends BaseSerializer {

  def writeMap(map: Map[Int, Any], byteBuf: ByteBuf): Unit = {
    val size: Int = map.size
    byteBuf.writeByte(size)
    map.foreach(kv => writeKV(kv._1, kv._2, byteBuf))
  }

  def readMap(byteBuf: ByteBuf): Map[Int, Any] = {
    val propNum: Int = byteBuf.readByte().toInt
    val propsMap: Map[Int, Any] = new Array[Int](propNum).map(item => {
      val propId: Int = byteBuf.readInt()
      val propType: Int = byteBuf.readByte().toInt
      val propValue = SerialzerDataType(propType) match {
        case SerialzerDataType.STRING => _readString(byteBuf)
        case SerialzerDataType.INT => byteBuf.readInt()
        case SerialzerDataType.LONG => byteBuf.readLong()
        case SerialzerDataType.DOUBLE => byteBuf.readDouble()
        case SerialzerDataType.FLOAT => byteBuf.readFloat()
        case SerialzerDataType.BOOLEAN => byteBuf.readBoolean()
        case SerialzerDataType.BLOB => _readBlob(byteBuf)
        case SerialzerDataType.DATE => _readDate(byteBuf)
//        case SerialzerDataType.DATE_TIME => _readDateTime(byteBuf)
        case SerialzerDataType.TIME => _readTime(byteBuf)
        case SerialzerDataType.LOCAL_DATE_TIME => _readLocalDateTime(byteBuf)
        case SerialzerDataType.LOCAL_TIME => _readLocalTime(byteBuf)

        case SerialzerDataType.ARRAY_STRING => ArraySerializer.readArray(byteBuf, propType).asInstanceOf[Array[String]]
        case SerialzerDataType.ARRAY_INT => ArraySerializer.readArray(byteBuf, propType).asInstanceOf[Array[Int]]
        case SerialzerDataType.ARRAY_LONG => ArraySerializer.readArray(byteBuf, propType).asInstanceOf[Array[Long]]
        case SerialzerDataType.ARRAY_DOUBLE => ArraySerializer.readArray(byteBuf, propType).asInstanceOf[Array[Double]]
        case SerialzerDataType.ARRAY_FLOAT => ArraySerializer.readArray(byteBuf, propType).asInstanceOf[Array[Float]]
        case SerialzerDataType.ARRAY_BOOLEAN => ArraySerializer.readArray(byteBuf, propType).asInstanceOf[Array[Boolean]]
        case SerialzerDataType.ARRAY_ANY => ArraySerializer.readArray(byteBuf, propType)

        case _ => _readString(byteBuf)
      }
      propId -> propValue
    }).toMap
    propsMap
  }

  def writeKV(keyId: Int, value: Any, byteBuf: ByteBuf): Any = {
    byteBuf.writeInt(keyId)
    value match {
      case s: Boolean => _writeBoolean(value.asInstanceOf[Boolean], byteBuf)
      case s: Double => _writeDouble(value.asInstanceOf[Double], byteBuf)
      case s: Float => _writeFloat(value.asInstanceOf[Float], byteBuf)
      case s: Int => _writeInt(value.asInstanceOf[Int], byteBuf)
      case s: Long => _writeLong(value.asInstanceOf[Long], byteBuf)
      case s: Array[_] => ArraySerializer.writeArray(value.asInstanceOf[Array[_]], byteBuf)
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

}
