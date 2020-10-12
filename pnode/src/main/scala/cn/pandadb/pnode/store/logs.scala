package cn.pandadb.pnode.store

import java.io.File

import io.netty.buffer.ByteBuf

import scala.collection.mutable

trait LogRecord {
}

class UnmergedLogs[T, Id, Position] {
  private val toAdd = mutable.Map[Id, T]()
  private val toDelete = mutable.Map[Id, Position]()

  def add(id: Id, t: T) = toAdd += id -> t

  def delete(id: Id, pos: Position) = toDelete += id -> pos

  def merge(): MergedLogs[T, Position] = {
    //toAdd={11,12,13}, toDelete={12,7,8,9}
    //intersection={12}
    val intersection = toDelete.keySet.intersect(toAdd.keySet)

    //toAdd={11,13}, toDelete={7,8,9}
    if(intersection.nonEmpty) {
      toAdd --= intersection
      toDelete --= intersection
    }

    //toReplace={11->7,13->8}, toAdd={}, toDelete={9}
    val toReplace: Iterable[(Position, T)] = toAdd.zip(toDelete).map(x => x._2._2 -> x._1._2)

    MergedLogs[T, Position](
      toAdd.drop(toReplace.size).map(_._2),
      toDelete.drop(toReplace.size).map(_._2),
      toReplace)
  }
}

case class MergedLogs[T, Position]
(
  toAdd: Iterable[T],
  toDelete: Iterable[Position],
  toReplace: Iterable[(Position, T)]
) {
  assert(!(toAdd.nonEmpty && toDelete.nonEmpty))
}

case class MergedGraphLogs
(
  nodes: MergedLogs[StoredNode, Long],
  rels: MergedLogs[StoredRelation, Long]
)

class FileBasedLogStore(logFile: File) {
  def length() = logFile.length()

  def offer[T](consume: (MergedGraphLogs => T)): T = {

    val nodelogs = new UnmergedLogs[StoredNode, Long, Long]()
    val rellogs = new UnmergedLogs[StoredRelation, Long, Long]()

    _store.loadAll().toArray.foreach {
      _ match {
        case CreateNode(t) =>
          nodelogs.add(t.id, t)

        case CreateRelation(t) =>
          rellogs.add(t.id, t)

        case DeleteNode(id, pos) =>
          nodelogs.delete(id, pos)

        case DeleteRelation(id, pos) =>
          rellogs.delete(id, pos)
      }
    }

    val graphLogs = MergedGraphLogs(nodelogs.merge(), rellogs.merge())
    val t = consume(graphLogs)
    _store.clear()
    t
  }

  def append(t: LogRecord) = _store.append(t, (t: LogRecord, pos: Long) => {})

  def append(ts: Iterable[LogRecord]) = _store.append(ts, (t: LogRecord, pos: Long) => {})

  def close() = _store.close()

  def clear() = _store.clear()

  val _store = new AppendingFileBasedSequenceStore[LogRecord] {
    val blockSerializer: ObjectBlockSerializer[LogRecord] = new VariantSizedObjectBlockSerializer[LogRecord] {
      override val objectSerializer: ObjectSerializer[LogRecord] = new ObjectSerializer[LogRecord] {
        override def readObject(buf: ByteBuf): LogRecord = {
          val mark = buf.readByte()
          mark match {
            case 1 =>
              CreateNode(NodeSerializer.readObject(buf))

            case 11 =>
              DeleteNode(buf.readLong(), buf.readLong())

            case 2 =>
              CreateRelation(RelationSerializer.readObject(buf))

            case 12 =>
              DeleteRelation(buf.readLong(), buf.readLong())
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

            case DeleteNode(id, pos) =>
              buf.writeByte(11)
              buf.writeLong(id)
              buf.writeLong(pos)

            case DeleteRelation(id, pos) =>
              buf.writeByte(12)
              buf.writeLong(id)
              buf.writeLong(pos)
          }
        }
      }
    }
    override val file: File = logFile
  }
}

case class CreateNode(t: StoredNode) extends LogRecord {

}

case class DeleteNode(id: Long, pos: Long) extends LogRecord {

}

case class CreateRelation(t: StoredRelation) extends LogRecord {

}

case class DeleteRelation(id: Long, pos: Long) extends LogRecord {

}

