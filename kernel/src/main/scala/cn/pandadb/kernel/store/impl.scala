package cn.pandadb.kernel.store

import java.io.File
import java.nio.charset.StandardCharsets

//import cn.pandadb.kernel.kv.StoredRelation
import cn.pandadb.kernel.util.StreamExLike._
import io.netty.buffer.ByteBuf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object NodeSerializer extends ObjectSerializer[StoredNode] {
  override def readObject(buf: ByteBuf): StoredNode =
    StoredNode(buf.readInt40(), (1 to buf.readableBytes()).map(_ => buf.readByte().toInt).toArray)

  override def writeObject(buf: ByteBuf, t: StoredNode): Unit = {
    buf.writeInt40(t.id)
    t.labelIds.foreach(buf.writeByte(_))
  }
}

object RelationSerializer extends ObjectSerializer[StoredRelation] {
  override def readObject(buf: ByteBuf): StoredRelation =
    StoredRelation(buf.readInt40(), buf.readInt40(), buf.readInt40(), buf.readInt())

  override def writeObject(buf: ByteBuf, t: StoredRelation): Unit = {
    buf.writeInt40(t.id).writeInt40(t.from).writeInt40(t.to).writeInt(t.typeId)
  }
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
  val _store = new FileBasedArrayStore[StoredLabel] {
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

    _store.saveAll(map.map(f => StoredLabel(f._1, f._2)).iterator, _ => {})
    newIds
  }
}

//case class StoredNode(id: Long, labelIds: Array[Int]=null) {
//}
//
//class StoredNodeWithProperty(override val id: Long,
//                             override val labelIds: Array[Int],
//                             val properties:Map[Int,Any])
//  extends StoredNode(id, labelIds){
//}

class StoredNodeWithProperty_tobe_deprecated(override val id: Long,
                                             override val labelIds: Array[Int],
                                             val properties:Map[String,Any])
  extends StoredNode(id, labelIds){
}

case class StoredLabel(key: String, value: Int) {

}

case class StoredRelation(id: Long, from: Long, to: Long, typeId: Int) {
}

class StoredRelationWithProperty(override val id: Long,
                                 override val from: Long,
                                 override val to: Long,
                                 override val typeId: Int,
                                 val properties:Map[Int,Any])
  extends StoredRelation(id, from, to, typeId) {
}

///////////////////////////////
trait StoreWithMap[T] {
  val mapFile: File
  val _mainStore: RemovableArrayStore[T]

  def openMapFile(): PositionMapFile

  def id(t: T): Long

  def loadAll(): Iterator[T] = _mainStore.loadAll()

  def merge(changes: MergedChanges[T, Long]): Unit = {
    if(changes.nonEmpty) {
      val map = openMapFile()
      changes.toDelete.foreach(id => {
        _mainStore.markDeleted(map.positionOf(id))
      })

      _mainStore.append(changes.toAdd, ts =>
        map.update(ts.map(x => id(x._1) -> x._2).iterator))

      map.close()
    }
  }

  def saveAll(ts: Iterator[T]): Unit = {
    val map = openMapFile()
    _mainStore.saveAll(ts, (ts) =>
      map.update(ts.map(x => id(x._1) -> x._2).iterator))

    map.close()
  }

  def close(): Unit = _mainStore.close()
}

trait XStore[Id, T] {
  def close(): Unit

  def loadAll(): Iterator[T]

  def merge(changes: MergedChanges[T, Id]): Unit

  def saveAll(ts: Iterator[T])
}

trait NodeStore extends XStore[Long, StoredNode] {
}

trait RelationStore extends XStore[Long, StoredRelation] {
}

class NodeStoreImpl(val nodeFile: File, val mapFile: File) extends NodeStore with StoreWithMap[StoredNode] {
  override val _mainStore = new FileBasedRemovableArrayStore[StoredNode] {
    override val file: File = nodeFile
    override val blockSerializer = new VariantSizedObjectBlockSerializer[StoredNode] {
      override val objectSerializer: ObjectSerializer[StoredNode] = NodeSerializer
      override val lengthSerializer: LengthSerializer = LengthSerializer.BYTE
    }
  }

  override def openMapFile = new PositionMapFile(mapFile)

  override def id(t: StoredNode): Long = t.id
}

class RelationStoreImpl(val relationFile: File, val mapFile: File) extends RelationStore with StoreWithMap[StoredRelation] {
  override val _mainStore = new FileBasedRemovableArrayStore[StoredRelation] {
    override val blockSerializer = new VariantSizedObjectBlockSerializer[StoredRelation] {
      override val objectSerializer: ObjectSerializer[StoredRelation] = RelationSerializer
      override val lengthSerializer: LengthSerializer = LengthSerializer.BYTE
    }
    override val file: File = relationFile
  }

  override def openMapFile = new PositionMapFile(mapFile)

  override def id(t: StoredRelation): Long = t.id
}

class TooManyLabelException(maxLimit: Int) extends RuntimeException