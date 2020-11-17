package cn.pandadb.kernel.store

import java.io.File
import java.nio.charset.StandardCharsets

import cn.pandadb.kernel.util.{AppendingFileBasedArrayStore, FileBasedPositionMappedArrayStore, ObjectBlockSerializer, ObjectSerializer, RandomAccessibleFileBasedArrayStore, VariantSizedObjectBlockSerializer}
import io.netty.buffer.ByteBuf

import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable}

object NodeSerializer extends ObjectSerializer[StoredNode] {
  override def readObject(buf: ByteBuf): StoredNode =
    StoredNode(buf.readLong(), buf.readByte(), buf.readByte(), buf.readByte(), buf.readByte())

  override def writeObject(buf: ByteBuf, t: StoredNode): Unit = {
    buf.writeLong(t.id).writeByte(t.labelId1).writeByte(t.labelId2).writeByte(t.labelId3).writeByte(t.labelId4)
  }
}

object RelationSerializer extends ObjectSerializer[StoredRelation] {
  override def readObject(buf: ByteBuf): StoredRelation =
    StoredRelation(buf.readLong(), buf.readLong(), buf.readLong(), buf.readInt())

  override def writeObject(buf: ByteBuf, t: StoredRelation): Unit =
    buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeInt(t.labelId)
}

object LabelSerializer extends ObjectSerializer[StoredLabel] {
  override def readObject(buf: ByteBuf): StoredLabel = {
    val length = buf.readInt()
    val key = buf.readCharSequence(length, StandardCharsets.UTF_8).toString
    val value = buf.readInt()
    StoredLabel(key, value)
  }

  override def writeObject(buf: ByteBuf, t: StoredLabel): Unit = {
    buf.writeInt(t.key.toCharArray.length)
    buf.writeCharSequence(t.key.toCharArray, StandardCharsets.UTF_8)
    buf.writeInt(t.value)
  }
}

/**
 * stores labels in format: id=label
 *
 * @param labelFile
 * @param max max value for id
 */
class LabelStore(labelFile: File, max: Int = Byte.MaxValue) {
  val _store = new AppendingFileBasedArrayStore[StoredLabel] {
    override val file: File = labelFile
    override val blockSerializer: ObjectBlockSerializer[StoredLabel] = new VariantSizedObjectBlockSerializer[StoredLabel] {
      override val objectSerializer: ObjectSerializer[StoredLabel] = new ObjectSerializer[StoredLabel] {
        override def readObject(buf: ByteBuf): StoredLabel = LabelSerializer.readObject(buf)

        override def writeObject(buf: ByteBuf, t: StoredLabel): Unit = LabelSerializer.writeObject(buf, t)
      }
    }
  }

  val map: mutable.Map[String, Int] = mutable.Map()
  _store.loadAll().toArray.foreach(f => map.put(f.key, f.value))

  def key(id: Int) = map.find(_._2 == id).map(_._1)

  def id(key: String): Int = {
    ids(Set(key)).head
  }

  def ids(keys: Set[String]): Set[Int] = {
    val ids = ArrayBuffer[Int]() ++ map.values.toArray
    val newIds = keys.map { key =>
      val opt = map.get(key)
      if (opt.isDefined) {
        opt.get
      }
      else {
        val newId = (1 to max).find(!ids.contains(_)).getOrElse(
          throw new TooManyLabelException(max))
        ids += newId
        map(key) = newId
        newId
      }
    }

    _store.saveAll(map.map(f => StoredLabel(f._1, f._2)).toSeq)
    newIds
  }
}

case class StoredNode(id: Long, labelId1: Int = 0, labelId2: Int = 0, labelId3: Int = 0, labelId4: Int = 0) {

}

case class StoredRelation(id: Long, from: Long, to: Long, labelId: Int) {

}

case class StoredLabel(key: String, value: Int) {

}

trait WithPositions[Id, Long, T] {
  private val mapId2Position = mutable.Map[Id, Long]()

  protected def id(t: T): Id

  protected def set(ts: Seq[(Long, T)]) = {
    mapId2Position.clear()
    mapId2Position ++= ts.map(x => id(x._2) -> x._1)
  }

  protected def position(t: T): Option[Long] = mapId2Position.get(id(t))
}

///////////////////////////////
class PositionMappedNodeStore(val nodeFile: File) extends NodeStore {
  val _store = new FileBasedPositionMappedArrayStore[StoredNode]() {
    override val objectSerializer: ObjectSerializer[StoredNode] = NodeSerializer2
    override val fixedSize: Int = 4
    override val file: File = nodeFile
  }

  override def loadAll(): Seq[StoredNode] = _store.loadAll().map(x => StoredNode(x._1, x._2.labelId1, x._2.labelId2, x._2.labelId3, x._2.labelId4))

  override def update(t: StoredNode): Unit = _store.update(t.id, t)

  override def saveAll(ts: Seq[StoredNode]): Unit = _store.saveAll(ts.map(x => x.id -> x))

  override def close(): Unit = _store.close()

  override def delete(id: Long): Unit = _store.markDeleted(id)
}

class PositionMappedRelationStore(val relationFile: File) extends RelationStore {
  val _store = new FileBasedPositionMappedArrayStore[StoredRelation]() {
    override val objectSerializer: ObjectSerializer[StoredRelation] = RelationSerializer2
    override val fixedSize: Int = 4 * 2 + 1
    override val file: File = relationFile
  }

  override def loadAll(): Seq[StoredRelation] = _store.loadAll().map(x => StoredRelation(x._1, x._2.from, x._2.to, x._2.labelId))

  override def update(t: StoredRelation): Unit = _store.update(t.id, t)

  override def saveAll(ts: Seq[StoredRelation]): Unit = _store.saveAll(ts.map(x => x.id -> x))

  override def close(): Unit = _store.close()

  override def delete(id: Long): Unit = _store.markDeleted(id)
}

object NodeSerializer2 extends ObjectSerializer[StoredNode] {
  override def readObject(buf: ByteBuf): StoredNode =
    StoredNode(-1, buf.readByte(), buf.readByte(), buf.readByte(), buf.readByte())

  override def writeObject(buf: ByteBuf, t: StoredNode): Unit = {
    buf.writeByte(t.labelId1).writeByte(t.labelId2).writeByte(t.labelId3).writeByte(t.labelId4)
  }
}

object RelationSerializer2 extends ObjectSerializer[StoredRelation] {
  override def readObject(buf: ByteBuf): StoredRelation =
    StoredRelation(buf.readLong(), buf.readLong(), buf.readLong(), buf.readInt())

  override def writeObject(buf: ByteBuf, t: StoredRelation): Unit =
    buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeInt(t.labelId)
}

class TooManyLabelException(maxLimit: Int) extends RuntimeException