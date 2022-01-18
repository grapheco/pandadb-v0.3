package org.grapheco.pandadb.kernel.util.serializer

import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.store.StoredNodeWithProperty
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 14:57 2020/12/17
 * @Modified By:
 */

// byteBuf [id][byte:labelsLen][labels]...[propNum][propId][proptype][propLen(ifNeed)][propValue]
// Map("String"->1, "Int" -> 2, "Long" -> 3, "Double" -> 4, "Float" -> 5, "Boolean" -> 6, "Array[String]" -> 7)
object NodeSerializer extends BaseSerializer {
  override val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  def serialize(labelId: Int, nodeId: Long): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeInt(labelId)
    byteBuf.writeLong(nodeId)
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def serialize(nodeValue: StoredNodeWithProperty): Array[Byte] = {
    serialize(nodeValue.id, nodeValue.labelIds, nodeValue.properties)
  }

  def serialize(id: Long, labels: Array[Int], prop: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeLong(id)
    _writeLabels(labels, byteBuf)
    byteBuf.writeByte(prop.size)
    prop.foreach(kv => _writeProp(kv._1, kv._2, byteBuf))
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def deserializeNodeValue(byteArr: Array[Byte]): StoredNodeWithProperty = {
    val byteBuf = Unpooled.wrappedBuffer(byteArr)
    val id = byteBuf.readLong()
    val labels: Array[Int] = _readLabels(byteBuf)
    val props: Map[Int, Any] = _readProps(byteBuf)
    byteBuf.release()
    new StoredNodeWithProperty(id, labels, props)
  }

//  def parallelDeserializeNodeKeyValue(iter: KeyValueIterator): Iterator[(Int, StoredNodeWithProperty)] = {
//    new PandaIteratorForKeyValueDeserializer[Int, StoredNodeWithProperty](iter, stepLength = 10000 * Runtime.getRuntime.availableProcessors()/8, _getLabelFromKey(_), deserializeNodeValue(_))
//  }

//  def parallelDeserializeNodeKeyValue(iter: Iterator[(Array[Byte], Array[Byte])]): Iterator[(Int, StoredNodeWithProperty)] = {
//    new PandaIteratorForKeyValueDeserializer[Int, StoredNodeWithProperty](iter, stepLength = 10000 * Runtime.getRuntime.availableProcessors()/8, _getLabelFromKey _, deserializeNodeValue(_))
//  }

//  def parallelDeserializeNodeValue(iter: KeyValueIterator): Iterator[StoredNodeWithProperty] = {
//    new PandaIteratorForValueDeSerializer[StoredNodeWithProperty](iter, stepLength = 10000 * Runtime.getRuntime.availableProcessors()/8, deserializeNodeValue(_))
//  }

//  def parallelDeserializeNodeValue(iter: Iterator[Array[Byte]], stepLength: Int): Iterator[StoredNodeWithProperty] = {
//    new PandaIteratorForValueDeSerializer[StoredNodeWithProperty](iter, stepLength , deserializeNodeValue(_))
//  }

  def deserializeNodeKey(byteArr: Array[Byte]): Long = {
    val byteBuf = Unpooled.wrappedBuffer(byteArr)
    byteBuf.readLong()
  }

  private def _writeLabels(labels: Array[Int], byteBuf: ByteBuf): Unit = {
    val len = labels.length
    byteBuf.writeByte(len)
    labels.foreach(label =>
      byteBuf.writeInt(label))
  }

  private def _writeProp(keyId: Int, value: Any, byteBuf: ByteBuf) = {
    MapSerializer.writeKV(keyId, value, byteBuf)
  }

   def _readLabels(byteBuf: ByteBuf): Array[Int] = {
    val len = byteBuf.readByte().toInt
    val labels: Array[Int] = new Array[Int](len).map(_ => byteBuf.readInt())
    labels
  }

   def _readProps(byteBuf: ByteBuf): Map[Int, Any] = {
    MapSerializer.readMap(byteBuf)
  }

  private def _getLabelFromKey(keyInBytes: Array[Byte]): Int = {
    ByteUtils.getInt(keyInBytes, 0)
  }
}