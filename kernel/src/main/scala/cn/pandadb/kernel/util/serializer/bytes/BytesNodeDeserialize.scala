package cn.pandadb.kernel.util.serializer.bytes

import cn.pandadb.kernel.store.StoredNodeWithProperty
object BytesNodeDeserialize {

  def deserializeNodeValue(data: Array[Byte]): StoredNodeWithProperty ={
    val id = DeserializeByteUtils.getLong(data, 0)
    val labelNum = DeserializeByteUtils.getByte(data, 8).toInt
    val labels = (0 until labelNum).map(i => DeserializeByteUtils.getInt(data, 9 + i * 4)).toArray
    val propsNum = DeserializeByteUtils.getByte(data, 9 + labelNum * 4)

    var propsIndex = 9 + labelNum * 4 + 1
    val propsMap = (0 until propsNum).map(i => {
      val propId = {
        val id = DeserializeByteUtils.getByte(data, propsIndex).toInt
        propsIndex += 1
        id
      }
      val propType = {
        val typeId = DeserializeByteUtils.getByte(data, propsIndex).toInt
        propsIndex += 1
        typeId
      }
      val propsValue = propType match {
        case 1 => {
          val res = readString(data, propsIndex)
          propsIndex = res._2
          res._1
        }
        case 2 => {
          val res = DeserializeByteUtils.getInt(data, propsIndex)
          propsIndex += 4
          res
        }
        case 3 => {
          val res = DeserializeByteUtils.getLong(data, propsIndex)
          propsIndex += 8
          res
        }
        case 4 => {
          val res = DeserializeByteUtils.getDouble(data, propsIndex)
          propsIndex += 8
          res
        }
        case 5 => {
          val res = DeserializeByteUtils.getFloat(data, propsIndex)
          propsIndex += 4
          res
        }
        case 6 => {
          val res = DeserializeByteUtils.getBoolean(data, propsIndex)
          propsIndex += 1
          res
        }
        case 7 => {
          val res = readAnyArray(data, propsIndex)
          propsIndex = res._2
          res._1
        }
        case _ =>{
          val res = readString(data, propsIndex)
          propsIndex = res._2
          res._1
        }
      }
      propId -> propsValue
    }).toMap
    new StoredNodeWithProperty(id, labels, propsMap)
  }

  private def readString(data: Array[Byte], startIndex: Int): (String, Int) ={
    val len = DeserializeByteUtils.getShort(data, startIndex).toInt
    val str = new String(data.slice(startIndex + 2, startIndex + 2 + len), "UTF-8")
    (str, startIndex + 2 + len)
  }

  private def readAnyArray(data: Array[Byte], startIndex: Int): (Array[Any], Int) = {
    var tmpIndex = startIndex
    val len = DeserializeByteUtils.getInt(data, tmpIndex)
    tmpIndex += 4
    val array = (0 until len).map(i => {
      val res = readAny(data, tmpIndex)
      tmpIndex = res._2
      res._1
    }).toArray
    (array, tmpIndex)
  }

  private def readAny(data: Array[Byte], startIndex: Int): (Any, Int) ={
    var tmpIndex = startIndex
    val typeId = data(tmpIndex)
    tmpIndex += 1
    val value = typeId match {
      case 1 => {
        val res = readString(data, tmpIndex)
        tmpIndex = res._2
        res._1
      }
      case 2 => {
        val res = DeserializeByteUtils.getInt(data, tmpIndex)
        tmpIndex += 4
        res
      }
      case 3 => {
        val res = DeserializeByteUtils.getLong(data, tmpIndex)
        tmpIndex += 8
        res
      }
      case 4 => {
        val res = DeserializeByteUtils.getDouble(data, tmpIndex)
        tmpIndex += 8
        res
      }
      case 5 => {
        val res = DeserializeByteUtils.getFloat(data, tmpIndex)
        tmpIndex += 4
        res
      }
      case 6 => {
        val res = DeserializeByteUtils.getBoolean(data, tmpIndex)
        tmpIndex += 1
        res
      }
    }
    (value, tmpIndex)
  }
}
