package cn.pandadb.kernel.kv

object PropertyValueConverter {
  def toBytes(v: Any): Array[Byte] = {
    v match {
      case v1: Long => ByteUtils.longToBytes(v1)
      case v1: Int => ByteUtils.intToBytes(v1)
      case v1: String => ByteUtils.stringToBytes(v1)
      case v1: AnyValue => toBytes(v1.anyValue)
      case v1: Any => throw new Exception(s"not support this value type: ${v1.getClass}")
    }
  }

}
