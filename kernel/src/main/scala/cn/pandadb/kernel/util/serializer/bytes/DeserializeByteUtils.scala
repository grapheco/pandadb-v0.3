package cn.pandadb.kernel.util.serializer.bytes

object DeserializeByteUtils {

  def getByte(memory: Array[Byte], index: Int): Byte = {
    memory(index)
  }

  def getShort(memory: Array[Byte], index: Int): Short = {
    (memory(index) << 8 | memory(index + 1) & 0xFF).toShort
  }

  def getInt(memory: Array[Byte], index: Int): Int = {
    (memory(index) & 0xff) << 24 |
      (memory(index + 1) & 0xff) << 16 |
      (memory(index + 2) & 0xff) << 8 |
      memory(index + 3) & 0xff
  }

  def getLong(memory: Array[Byte], index: Int): Long = {
    (memory(index).toLong & 0xff) << 56 |
      (memory(index + 1).toLong & 0xff) << 48 |
      (memory(index + 2).toLong & 0xff) << 40 |
      (memory(index + 3).toLong & 0xff) << 32 |
      (memory(index + 4).toLong & 0xff) << 24 |
      (memory(index + 5).toLong & 0xff) << 16 |
      (memory(index + 6).toLong & 0xff) << 8 |
      memory(index + 7).toLong & 0xff
  }
  def getDouble(memory: Array[Byte], index: Int): Double ={
    java.lang.Double.longBitsToDouble(getLong(memory, index))
  }
  def getFloat(memory: Array[Byte], index: Int): Float = {
    java.lang.Float.intBitsToFloat(getInt(memory, index))
  }
  def getBoolean(memory: Array[Byte], index: Int): Boolean = {
    getByte(memory, index) != 0
  }
}
