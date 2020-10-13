package cn.pandadb.pnode.store

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties
import io.netty.buffer.ByteBuf
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

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

/**
 * stores labels in format: id=label
 *
 * @param file
 * @param max max value for id
 */
class LabelStore(file: File, max: Int = Byte.MaxValue) {
  val map: mutable.Map[String, Int] = {
    val props = new Properties()
    val is = new FileInputStream(file)
    props.load(is)
    is.close()
    mutable.Map[String, Int]() ++ JavaConversions.propertiesAsScalaMap(props).map(x => x._1 -> x._2.toInt)
  }

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

    flush()
    newIds
  }

  private def flush() = {
    val props = new Properties()
    props.putAll(JavaConversions.mapAsJavaMap(map.map(x => x._1 -> x._2.toString)))
    val os = new FileOutputStream(file)
    props.save(os, "")
    os.close()
  }
}

case class StoredNode(id: Long, labelId1: Int = 0, labelId2: Int = 0, labelId3: Int = 0, labelId4: Int = 0) {

}

case class StoredRelation(id: Long, from: Long, to: Long, labelId: Int) {

}

class NodeStore(val file: File)  extends RandomAccessibleFileBasedSequenceStore[StoredNode] {
    override val objectSerializer: ObjectSerializer[StoredNode] = NodeSerializer
    override val fixedSize: Int = 8 + 4
}

class RelationStore(val file: File) extends RandomAccessibleFileBasedSequenceStore[StoredRelation] {
  override val objectSerializer: ObjectSerializer[StoredRelation] = RelationSerializer
  override val fixedSize: Int = 8 * 3 + 4
}

class TooManyLabelException(maxLimit: Int) extends RuntimeException