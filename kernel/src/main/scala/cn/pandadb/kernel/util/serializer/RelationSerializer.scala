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

  def serialize(relationId: Long, fromId: Long, toId: Long, typeId: Int, category: Int, props: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeLong(relationId)
    byteBuf.writeLong(fromId)
    byteBuf.writeLong(toId)
    byteBuf.writeByte(typeId)
    byteBuf.writeInt(category)
    _writeMap(props, byteBuf)
    val bytes = _exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def serialize(relation: StoredRelationWithProperty): Array[Byte] = {
    serialize(relation.id, relation.from, relation.to, relation.typeId, relation.typeId, relation.properties)
  }

  def deserializeRelWithProps(bytesArray: Array[Byte]): StoredRelationWithProperty = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArray)
    val relationId: Long = byteBuf.readLong()
    val fromId: Long = byteBuf.readLong()
    val toId: Long = byteBuf.readLong()
    val typeId: Int = byteBuf.readByte().toInt
    val category: Int = byteBuf.readInt()
    val props: Map[Int, Any] = _readMap(byteBuf)
    byteBuf.release()
    new StoredRelationWithProperty(relationId, fromId, toId, typeId, category, props)
  }

  def deserializeRelWithoutProps(bytesArray: Array[Byte]): StoredRelation = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArray)
    val relationId: Long = byteBuf.readLong()
    val fromId: Long = byteBuf.readLong()
    val toId: Long = byteBuf.readLong()
    val typeId: Int = byteBuf.readByte().toInt
    val category: Int = byteBuf.readInt()
    StoredRelation(relationId, fromId, toId, typeId, category)
  }

  // todo: add below funcs
  // only from
  // only to
}
