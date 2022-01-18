package org.grapheco.pandadb.kernel.util.serializer

import io.netty.buffer.ByteBuf

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 18:56 2021/4/25
 * @Modified By:
 */

object ArraySerializer extends BaseSerializer {
  // Serialized Array: [type-byte][len-int][...content...]

  def writeArray(array: Array[_], byteBuf: ByteBuf): Unit = {

    array match {
      case s: Array[String] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_STRING.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.asInstanceOf[Array[String]].foreach(item => _writeStringWithoutTypeFlag(item, byteBuf))
      }
      case s: Array[Int] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_INT.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.asInstanceOf[Array[Int]].foreach(item => byteBuf.writeInt(item))
      }
      case s: Array[Long] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_LONG.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.asInstanceOf[Array[Long]].foreach(item => byteBuf.writeLong(item))
      }
      case s: Array[Double] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_DOUBLE.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.asInstanceOf[Array[Double]].foreach(item => byteBuf.writeDouble(item))
      }
      case s: Array[Float] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_FLOAT.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.asInstanceOf[Array[Float]].foreach(item => byteBuf.writeFloat(item))
      }
      case s: Array[Boolean] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_BOOLEAN.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.asInstanceOf[Array[Boolean]].foreach(item => byteBuf.writeBoolean(item))
      }

      case s: Array[_] => {
        byteBuf.writeByte(SerialzerDataType.ARRAY_ANY.id)
        val len: Int = array.length
        byteBuf.writeInt(len)
        array.foreach(item => _writeAny(item, byteBuf))
      }
    }
  }

  def readArray[T](byteBuf: ByteBuf): Array[_] = {
    val typeFlag = byteBuf.readByte().toInt
    val len = byteBuf.readInt()
    SerialzerDataType(typeFlag) match {
      case SerialzerDataType.ARRAY_STRING => new Array[String](len).map(item => _readString(byteBuf))
      case SerialzerDataType.ARRAY_INT => new Array[Int](len).map(item => byteBuf.readInt())
      case SerialzerDataType.ARRAY_LONG => new Array[Long](len).map(item => byteBuf.readLong())
      case SerialzerDataType.ARRAY_DOUBLE => new Array[Double](len).map(item => byteBuf.readDouble())
      case SerialzerDataType.ARRAY_FLOAT => new Array[Float](len).map(item => byteBuf.readFloat())
      case SerialzerDataType.ARRAY_BOOLEAN => new Array[Boolean](len).map(item => byteBuf.readBoolean())
      case SerialzerDataType.ARRAY_ANY => new Array[Any](len).map(item => _readAny(byteBuf))
    }
  }

  def readArray[T](byteBuf: ByteBuf, typeFlag: Int): Array[_] = {
    val len = byteBuf.readInt()
    SerialzerDataType(typeFlag) match {
      case SerialzerDataType.ARRAY_STRING => new Array[String](len).map(item => _readString(byteBuf))
      case SerialzerDataType.ARRAY_INT => new Array[Int](len).map(item => byteBuf.readInt())
      case SerialzerDataType.ARRAY_LONG => new Array[Long](len).map(item => byteBuf.readLong())
      case SerialzerDataType.ARRAY_DOUBLE => new Array[Double](len).map(item => byteBuf.readDouble())
      case SerialzerDataType.ARRAY_FLOAT => new Array[Float](len).map(item => byteBuf.readFloat())
      case SerialzerDataType.ARRAY_BOOLEAN => new Array[Boolean](len).map(item => byteBuf.readBoolean())
      case SerialzerDataType.ARRAY_ANY => new Array[Any](len).map(item => _readAny(byteBuf))
    }
  }

  //Note: should not write type flag in this function
  private def _writeStringWithoutTypeFlag(str: String, byteBuf: ByteBuf): Unit = {
    val strInBytes: Array[Byte] = str.getBytes
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

}
