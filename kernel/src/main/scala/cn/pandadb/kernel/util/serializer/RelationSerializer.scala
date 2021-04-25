package cn.pandadb.kernel.util.serializer

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

  override def serialize(relationId: Long): Array[Byte] = {
    BaseSerializer.serialize(relationId)
  }

  def serialize(relation: StoredRelationWithProperty): Array[Byte] = {
    serialize(relation.id, relation.from, relation.to, relation.typeId, relation.properties)
  }

  def serialize(relationId: Long, fromId: Long, toId: Long, typeId: Int, props: Map[Int, Any]): Array[Byte] = {
    val byteBuf: ByteBuf = allocator.heapBuffer()
    byteBuf.writeLong(relationId)
    byteBuf.writeLong(fromId)
    byteBuf.writeLong(toId)
    byteBuf.writeByte(typeId)
    MapSerializer.writeMap(props, byteBuf)
    val bytes = exportBytes(byteBuf)
    byteBuf.release()
    bytes
  }

  def deserializeRelWithProps(bytesArray: Array[Byte]): StoredRelationWithProperty = {
    val byteBuf: ByteBuf = Unpooled.wrappedBuffer(bytesArray)
    val relationId: Long = byteBuf.readLong()
    val fromId: Long = byteBuf.readLong()
    val toId: Long = byteBuf.readLong()
    val typeId: Int = byteBuf.readByte().toInt
    val props: Map[Int, Any] = MapSerializer.readMap(byteBuf)
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
}