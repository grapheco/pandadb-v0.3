package cn.pandadb.pnode.store

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Properties

import cn.pandadb.pnode.util.StreamExLike._
import io.netty.buffer.ByteBuf

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

class FileBasedLabelStore(file: File) {
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
        val newId = (1 to 255).find(!ids.contains(_)).getOrElse(
          throw new TooManyLabelException(255))
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

class FileBasedLogStore(val file: File) extends AppendingFileBasedSequenceStore[LogRecord] {
  val obss: ObjectBlockSerializationStrategy[LogRecord] = new VariantSizedObjectBlockSerializationStrategy[LogRecord] {
    override val orw: ObjectSerializer[LogRecord] = new ObjectSerializer[LogRecord] {
      override def readObject(buf: ByteBuf): LogRecord = {
        val mark = buf.readByte()
        mark match {
          case 1 =>
            CreateNode(StoredNode(buf.readLong(), buf.readInt(), buf.readInt(), buf.readInt(), buf.readInt()))

          case 11 =>
            DeleteNode(buf.readLong())

          case 2 =>
            CreateRelation(StoredRelation(buf.readLong(), buf.readLong(), buf.readLong(), buf.readInt()))

          case 12 =>
            DeleteRelation(buf.readLong())
        }
      }

      override def writeObject(buf: ByteBuf, t: LogRecord): Unit = {
        t match {
          case CreateNode(t) =>
            buf.writeByte(1)
            buf.writeLong(t.id)

          case CreateRelation(t) =>
            buf.writeByte(2)
            buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeInt(t.labelId)

          case DeleteNode(id) =>
            buf.writeByte(11)
            buf.writeLong(id)

          case DeleteRelation(id) =>
            buf.writeByte(12)
            buf.writeLong(id)
        }
      }
    }
  }
}

case class CreateNode(t: StoredNode) extends LogRecord {

}

case class DeleteNode(id: Long) extends LogRecord {

}

case class CreateRelation(t: StoredRelation) extends LogRecord {

}

case class DeleteRelation(id: Long) extends LogRecord {

}

case class StoredNode(id: Long, labelId1: Int = 0, labelId2: Int = 0, labelId3: Int = 0, labelId4: Int = 0) {

}

case class StoredRelation(id: Long, from: Long, to: Long, labelId: Int) {

}

class FileBasedNodeStore(val file: File) extends RemovableFileBasedSequenceStore[StoredNode] {
  override val orw: ObjectSerializer[StoredNode] = new ObjectSerializer[StoredNode] {
    override def readObject(buf: ByteBuf): StoredNode =
      StoredNode(buf.readLong(), buf.readByte(), buf.readByte(), buf.readByte(), buf.readByte())

    override def writeObject(buf: ByteBuf, t: StoredNode): Unit = {
      buf.writeLong(t.id).writeByte(t.labelId1).writeByte(t.labelId2).writeByte(t.labelId3).writeByte(t.labelId4)
    }
  }

  override val fixedSize: Int = 8 + 4
}

class FileBasedRelationStore(val file: File) extends RemovableFileBasedSequenceStore[StoredRelation] {
  override val orw: ObjectSerializer[StoredRelation] = new ObjectSerializer[StoredRelation] {
    override def readObject(buf: ByteBuf): StoredRelation =
      StoredRelation(buf.readLong(), buf.readLong(), buf.readLong(), buf.readByte())

    override def writeObject(buf: ByteBuf, t: StoredRelation): Unit =
      buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeByte(t.labelId)
  }

  override val fixedSize: Int = 8 * 3 + 1
}

class TooManyLabelException(maxLimit: Int) extends RuntimeException