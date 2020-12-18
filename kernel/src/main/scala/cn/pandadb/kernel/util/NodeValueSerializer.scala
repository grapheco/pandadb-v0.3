package cn.pandadb.kernel.util

import cn.pandadb.kernel.kv.NodeValue
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 14:57 2020/12/17
 * @Modified By:
 */

// byteBuf [id][byte:labelsLen][labels]...[propNum][propId][proptype][propLen(ifNeed)][propValue]
// Map("String"->1, "Int" -> 2, "Long" -> 3, "Double" -> 4, "Float" -> 5, "Boolean" -> 6)
class NodeValueSerializer extends BaseSerializer {
  override val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  def serialize(nodeValue: NodeValue): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeLong(nodeValue.id)
    _writeLabels(nodeValue.labelIds, byteBuf)
    byteBuf.writeByte(nodeValue.properties.size)
    nodeValue.properties.foreach(kv => _writeProp(kv._1, kv._2, byteBuf))
    val dst = new Array[Byte](byteBuf.writerIndex())
    byteBuf.readBytes(dst)
    byteBuf.release()
    dst
  }

  def deserialize(byteArr: Array[Byte]): NodeValue = {
    val byteBuf = Unpooled.wrappedBuffer(byteArr)
    val id = byteBuf.readLong()
    val labels: Array[Int] = _readLabels(byteBuf)
    val props: Map[Int, Any] = _readProps(byteBuf)
    byteBuf.release()
    new NodeValue(id, labels, props)

  }

  private def _writeLabels(labels: Array[Int], byteBuf: ByteBuf): Unit = {
    val len = labels.length
    byteBuf.writeByte(len)
    labels.foreach(label =>
      byteBuf.writeInt(label))
  }

  private def _writeProp(keyId: Int, value: Any, byteBuf: ByteBuf) = {
    _writeKV(keyId, value, byteBuf)
  }

  private def _readLabels(byteBuf: ByteBuf): Array[Int] = {
    val len = byteBuf.readByte().toInt
    val labels: Array[Int] = new Array[Int](len).map(_ => byteBuf.readInt())
    labels
  }

  private def _readProps(byteBuf: ByteBuf): Map[Int, Any] = {
    _bytes2Map(byteBuf)
  }
}