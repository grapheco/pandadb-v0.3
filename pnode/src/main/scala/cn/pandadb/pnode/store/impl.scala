package cn.pandadb.pnode.store

import java.io.File
import cn.pandadb.pnode.util.StreamExLike._
import io.netty.buffer.ByteBuf

class FileBasedLogStore(val file: File) extends FileBasedSequenceStore[LogRecord] {
  override def readObject(buf: ByteBuf): LogRecord = {
    val mark = buf.readByte()
    mark match {
      case 1 =>
        CreateNode(Node(buf.readLong()))

      case 11 =>
        DeleteNode(buf.readLong())

      case 2 =>
        CreateRelation(Relation(buf.readLong(), buf.readLong(), buf.readLong(), buf.readString()))

      case 12 =>
        DeleteRelation(buf.readLong())
    }
  }

  override def getFile: File = file

  override def writeObject(buf: ByteBuf, t: LogRecord): Unit = {
    t match {
      case CreateNode(t) =>
        buf.writeByte(1)
        buf.writeLong(t.id)

      case CreateRelation(t) =>
        buf.writeByte(2)
        buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeString(t.label)

      case DeleteNode(id) =>
        buf.writeByte(11)
        buf.writeLong(id)

      case DeleteRelation(id) =>
        buf.writeByte(12)
        buf.writeLong(id)
    }
  }
}

case class CreateNode(t: Node) extends LogRecord {

}

case class DeleteNode(id: Long) extends LogRecord {

}

case class CreateRelation(t: Relation) extends LogRecord {

}

case class DeleteRelation(id: Long) extends LogRecord {

}

case class Node(id: Long, labels: String*) {

}

case class Relation(id: Long, from: Long, to: Long, label: String) {

}

class FileBasedNodeStore(seqFile: File) extends FileBasedSequenceStore[Node] {

  override def readObject(buf: ByteBuf): Node =
    Node(buf.readLong(), buf.readRemainingAsString().split(";").toSeq: _*)

  override def getFile: File = seqFile

  override def writeObject(buf: ByteBuf, t: Node): Unit =
    buf.writeLong(t.id).writeString(t.labels.mkString(";"), false)
}

class FileBasedRelationStore(seqFile: File) extends FileBasedSequenceStore[Relation] {
  override def getFile: File = seqFile

  override def readObject(buf: ByteBuf): Relation =
    Relation(buf.readLong(), buf.readLong(), buf.readLong(), buf.readRemainingAsString())

  override def writeObject(buf: ByteBuf, t: Relation): Unit =
    buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeString(t.label, false)
}
