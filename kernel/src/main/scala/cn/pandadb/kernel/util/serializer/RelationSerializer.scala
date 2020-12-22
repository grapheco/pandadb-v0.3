package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.kv.KeyHandler.KeyType
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}
import io.netty.buffer.{ByteBuf, ByteBufAllocator, Unpooled}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 17:19 2020/12/18
 * @Modified By:
 */
object RelationSerializer extends BaseSerializer {
  override val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  // [keyType(1Byte),relationId(8Bytes)]
  def serialize(edgeId: Long): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeByte(KeyType.Relation.id.toByte)
    byteBuf.writeLong(edgeId)
    val bytes = _exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def serialize(r: StoredRelationWithProperty): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeLong(r.id)
    byteBuf.writeLong(r.from)
    byteBuf.writeLong(r.to)
    byteBuf.writeByte(r.typeId)
    _writeMap(r.properties, byteBuf)
    val bytes = _exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def deserializeRelWithProps(bytesArray: Array[Byte]): StoredRelationWithProperty = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArray)
    val relationId: Long = byteBuf.readLong()
    val fromId: Long = byteBuf.readLong()
    val toId: Long = byteBuf.readLong()
    val typeId: Int = byteBuf.readByte().toInt
    val props: Map[Int, Any] = _readMap(byteBuf)
    byteBuf.release()
    new StoredRelationWithProperty(relationId, fromId, toId, typeId, props)
  }

  def deserializeRelWithoutProps(bytesArray: Array[Byte]): StoredRelation = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArray)
    val relationId: Long = byteBuf.readLong()
    val fromId: Long = byteBuf.readLong()
    val toId: Long = byteBuf.readLong()
    val typeId: Int = byteBuf.readByte().toInt
    StoredRelation(relationId, fromId, toId, typeId)
  }

  // todo: add below funcs
  // only from
  // only to


}
