package cn.pandadb.kernel.store

import java.io.File
import io.netty.buffer.ByteBuf
import scala.collection.mutable

trait LogRecord {
}

class UnmergedChanges[T, Id] {
  private val toAdd = mutable.Map[Id, T]()
  private val toDelete = mutable.Map[Id, Id]()

  def add(id: Id, t: T) = toAdd += id -> t

  def delete(id: Id) = toDelete += id -> id

  def merge(): MergedChanges[T, Id] = {
    //toAdd={11,12,13}, toDelete={12,7,8,9}
    //intersection={12}
    val intersection = toDelete.keySet.intersect(toAdd.keySet)

    //toAdd={11,13}, toDelete={7,8,9}
    if (intersection.nonEmpty) {
      toAdd --= intersection
      toDelete --= intersection
    }

    //toReplace={11->7,13->8}, toAdd={}, toDelete={9}
    //val toReplace: Seq[(Id, T)] = toAdd.zip(toDelete).map(x => x._2._2 -> x._1._2).toSeq

    MergedChanges[T, Id](
      toAdd.map(_._2).toSeq,
      toDelete.map(_._2).toSeq)
  }
}

case class MergedChanges[T, Id]
(
  toAdd: Seq[T],
  toDelete: Seq[Id],
) {
  assert(!(toAdd.nonEmpty && toDelete.nonEmpty))
}

case class MergedGraphChanges
(
  nodes: MergedChanges[StoredNode, Long],
  rels: MergedChanges[StoredRelation, Long]
)

class LogStore(logFile: File) {
  def length() = logFile.length()

  def offer[T](consume: (MergedGraphChanges => T)): T = {

    val nodelogs = new UnmergedChanges[StoredNode, Long]()
    val rellogs = new UnmergedChanges[StoredRelation, Long]()

    _store.loadAll().toArray.foreach {
      _ match {
        case CreateNode(t) =>
          nodelogs.add(t.id, t)

        case CreateRelation(t) =>
          rellogs.add(t.id, t)

        case DeleteNode(id) =>
          nodelogs.delete(id)

        case DeleteRelation(id) =>
          rellogs.delete(id)
      }
    }

    val graphLogs = MergedGraphChanges(nodelogs.merge(), rellogs.merge())
    val t = consume(graphLogs)
    _store.clear()
    t
  }

  def append(t: LogRecord) = _store.append(Some(t), _ => {})

  def append(ts: Iterable[LogRecord]) = _store.append(ts, _ => {})

  def close() = _store.close()

  def clear() = _store.clear()

  val _store = new FileBasedArrayStore[LogRecord] {
    val blockSerializer: ObjectBlockSerializer[LogRecord] = new VariantSizedObjectBlockSerializer[LogRecord] {
      override val objectSerializer: ObjectSerializer[LogRecord] = new ObjectSerializer[LogRecord] {
        override def readObject(buf: ByteBuf): LogRecord = {
          val mark = buf.readByte()
          mark match {
            case 1 =>
              CreateNode(NodeSerializer.readObject(buf))

            case 11 =>
              DeleteNode(buf.readLong())

            case 2 =>
              CreateRelation(RelationSerializer.readObject(buf))

            case 12 =>
              DeleteRelation(buf.readLong())
          }
        }

        override def writeObject(buf: ByteBuf, r: LogRecord): Unit = {
          r match {
            case CreateNode(t) =>
              buf.writeByte(1)
              NodeSerializer.writeObject(buf, t)

            case CreateRelation(t) =>
              buf.writeByte(2)
              RelationSerializer.writeObject(buf, t)

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
    override val file: File = logFile
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

