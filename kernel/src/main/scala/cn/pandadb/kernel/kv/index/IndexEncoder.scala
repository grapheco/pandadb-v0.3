package cn.pandadb.kernel.kv.index

import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.util.serializer.BaseSerializer

/**
 * @ClassName IndexEncoder
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/21
 * @Version 0.1
 */
object IndexEncoder {

  val NULL: Byte = -1
  val FALSE_CODE: Byte = 0
  val TRUE_CODE: Byte = 1
  val INT_CODE: Byte = 2
  val FLOAT_CODE: Byte = 3
  val LONG_CODE: Byte = 4
  val STRING_CODE: Byte = 5
  val MAP_CODE: Byte = 6

  /**
   * support dataType : boolean, byte, short, int, float, long, double, string
   * ╔═══════════════╦══════════════╦══════════════╗
   * ║    dataType   ║  storageType ║    typeCode  ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Null     ║    Empty     ║      -1      ║  ==> TODO
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      False    ║    Empty     ║      0       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      True     ║    Empty     ║      1       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Byte     ║    *Int*     ║      2       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Short    ║    *Int*     ║      2       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Int      ║     Int      ║      2       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Float    ║    Float     ║      3       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║     Double    ║   *Float*    ║      3       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║      Long     ║     Long     ║      4       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║     String    ║    String    ║      5       ║
   * ╠═══════════════╬══════════════╬══════════════╣
   * ║       Map     ║  ByteArray   ║      6       ║
   * ╚═══════════════╩══════════════╩══════════════╝
   *
   * @param data the data to encode
   * @return the bytes array after encode
   */
  def encode(data: Any): Array[Byte] = {
    data match {
      //case data == null => Array.emptyByteArray
      case data: Boolean => Array.emptyByteArray
      case data: Byte => intEncode(data.toInt)
      case data: Short => intEncode(data.toInt)
      case data: Int => intEncode(data)
      case data: Float => floatEncode(data)
      case data: Long => intEncode(data.toInt)// fixme
      case data: Double => floatEncode(data.toFloat)
      case data: String => stringEncode(data)
      case data: Map[Int, Any] => BaseSerializer.map2Bytes(data)
      case data: Any => throw new Exception(s"this value type: ${data.getClass} is not supported")
    }
  }

  def typeCode(data: Any): Byte = {
    data match {
      //case data == null => NULL
      case data: Boolean if data => TRUE_CODE
      case data: Boolean if !data => FALSE_CODE
      case data: Byte => INT_CODE
      case data: Short => INT_CODE
      case data: Int => INT_CODE
      case data: Float => FLOAT_CODE
      case data: Long => INT_CODE // fixme
      case data: Double => FLOAT_CODE
      case data: String => STRING_CODE
      case data: Map[Int, Any] => MAP_CODE
      case data: Any => throw new Exception(s"this value type: ${data.getClass} is not supported")
    }
  }


  private def intEncode(int: Int): Array[Byte] = {
    ByteUtils.intToBytes(int ^ Int.MinValue)
  }

  /**
   * if float greater than or equal 0, the highest bit set to 1
   * else NOT each bit
   */
  private def floatEncode(float: Float): Array[Byte] = {
    val buf = ByteUtils.floatToBytes(float)
    if (float >= 0) {
      // 0xxxxxxx => 1xxxxxxx
      ByteUtils.setInt(buf, 0, ByteUtils.getInt(buf, 0) ^ Int.MinValue)
    } else {
      // ~
      ByteUtils.setInt(buf, 0, ~ByteUtils.getInt(buf, 0))
    }
    buf
  }

  private def longEncode(long: Long): Array[Byte] = {
    ByteUtils.longToBytes(long ^ Long.MinValue)
  }

  private def stringEncode_old(string: String): Array[Byte] = {
    ByteUtils.stringToBytes(string)
  }


  /**
   * The string is divided into groups according to 8 bytes.
   * The last group is less than 8 bytes, and several zeros bytes are supplemented.
   * Add a byte to the end of each group. The value of the byte is 255 minus the number of 0 bytes filled by the group.
   * eg:
   * []                   =>  [0,0,0,0,0,0,0,0,247]
   * [1,2,3]              =>  [1,2,3,0,0,0,0,0,250]
   * [1,2,3,0]            =>  [1,2,3,0,0,0,0,0,251]
   * [1,2,3,4,5,6,7,8]    =>  [1,2,3,4,5,6,7,8,255,0,0,0,0,0,0,0,0,247]
   * [1,2,3,4,5,6,7,8,9]  =>  [1,2,3,4,5,6,7,8,255,9,0,0,0,0,0,0,0,248]
   */
  private def stringEncode(string: String): Array[Byte] = {
    val buf = ByteUtils.stringToBytes(string)
    val group = buf.length / 8 + 1
    val res = new Array[Byte](group * 9)
    // set value Bytes
    for (i <- buf.indices)
      res(i + i / 8) = buf(i)
    // set length Bytes
    for (i <- 1 until group)
      res(9 * i - 1) = 255.toByte
    // set last Bytes
    res(res.length - 1) = (247 + buf.length % 8).toByte
    res
  }
}

