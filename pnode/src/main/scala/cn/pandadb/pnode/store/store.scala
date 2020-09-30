package cn.pandadb.pnode.store

import java.io.{EOFException, File, RandomAccessFile}

import cn.pandadb.pnode.util.StreamExLike._
import io.netty.buffer.{ByteBuf, Unpooled}

trait LogRecord {
}

trait LogStore {
  def offer(batchSize: Int, consume: Stream[LogRecord] => Unit)

  def append(log: LogRecord)
}

/**
 * [d][llll][...]
 * d-deleted flag
 * l-length of block (excluding d & l)
 */
trait FileBasedSequenceReader[T] {
  def getFile: File

  def readObject(buf: ByteBuf): T

  private def createStream(ptr: RandomAccessFile, offset: Long, maxCount: Int): Stream[(Long, Long, T)] = {
    if (maxCount == 0) {
      Stream.empty[(Long, Long, T)]
    }
    else {
      try {
        val deleted = ptr.readByte()
        val len = ptr.readInt()

        if (deleted != 0) {
          ptr.skipBytes(len)
          createStream(ptr, offset + 1 + 4 + len, maxCount)
        }
        else {
          val bytes = new Array[Byte](len)
          ptr.read(bytes)
          val buf = Unpooled.wrappedBuffer(bytes)
          val t = readObject(buf)
          Stream.cons(Tuple3(offset, offset + 1 + 4 + len, t), {
            createStream(ptr, offset + 1 + 4 + len, maxCount - 1)
          })
        }
      }
      catch {
        case _: EOFException => {
          ptr.close()
          Stream.empty[(Long, Long, T)]
        }
        case e => throw e
      }
    }
  }

  /**
   * return object list with position(start, end+1)
   *
   * @param maxCount
   * @return
   */
  def listWithPosition(maxCount: Int = -1): Stream[(Long, Long, T)] = {
    val ptr = new RandomAccessFile(getFile, "r")
    createStream(ptr, 0, maxCount)
  }

  def list(maxCount: Int = -1): Stream[T] = {
    listWithPosition(maxCount).map(_._3)
  }
}

/**
 * [d][llll][...]
 * d-deleted flag
 * l-length of block (excluding d & l)
 */
trait FileBasedSequenceWriter[T] {
  def getFile(): File

  lazy val ptr = new RandomAccessFile(getFile, "rw")

  def writeObject(buf: ByteBuf, t: T)

  protected def writeObjectBlock(buf: ByteBuf, t: T): Unit = {
    buf.writeByte(0) //deleted
    buf.writeInt(0) //length
    writeObject(buf, t)
    val len = buf.readableBytes()
    buf.setInt(1, len - 1 - 4)
  }

  def append(t: Iterator[T]): Unit = {
    val buf = Unpooled.buffer()
    t.foreach(writeObjectBlock(buf, _))
    ptr.seek(ptr.length())
    ptr.write(buf.array().slice(0, buf.readableBytes()))
  }

  def append(t: T): Unit = {
    val buf = Unpooled.buffer()
    writeObjectBlock(buf, t)
    ptr.seek(ptr.length())
    ptr.write(buf.array().slice(0, buf.readableBytes()))
  }

  def markDeleted(pos: Long): Unit = {
    ptr.seek(pos)
    ptr.writeByte(1)
  }
}

class FileBasedLogStreamReader(val file: File) extends FileBasedSequenceReader[LogRecord] {
  override def readObject(buf: ByteBuf): LogRecord = {
    val mark = buf.readByte()
    mark match {
      case 1 =>
        CreateNode(Node(buf.readLong()))

      case 11 =>
        DeleteNode(buf.readLong())

      case 2 =>
        CreateRelation(Relation(buf.readLong(), buf.readLong(), buf.readLong()))

      case 12 =>
        DeleteRelation(buf.readLong())
    }
  }

  override def getFile: File = file
}

class FileBasedLogStreamWriter(val file: File) extends FileBasedSequenceWriter[LogRecord] {
  override def writeObject(buf: ByteBuf, t: LogRecord): Unit = {
    t match {
      case CreateNode(t) =>
        buf.writeByte(1)
        buf.writeLong(t.id)

      case CreateRelation(t) =>
        buf.writeByte(2)
        buf.writeLong(t.id).writeLong(t.from).writeLong(t.to)

      case DeleteNode(id) =>
        buf.writeByte(11)
        buf.writeLong(id)

      case DeleteRelation(id) =>
        buf.writeByte(12)
        buf.writeLong(id)
    }
  }

  override def getFile(): File = file
}

case class CreateNode(t: Node) extends LogRecord {

}

case class DeleteNode(id: Long) extends LogRecord {

}

case class CreateRelation(t: Relation) extends LogRecord {

}

case class DeleteRelation(id: Long) extends LogRecord {

}

class FileBasedLogStore(file: File) extends LogStore {
  def list(maxCount: Int = -1): Stream[LogRecord] = {
    val streamReader = new FileBasedLogStreamReader(file)
    streamReader.list(maxCount: Int)
  }

  override def offer(batchSize: Int, consume: Stream[LogRecord] => Unit): Unit = {
    var stream: Stream[LogRecord] = Stream.empty[LogRecord]
    do {
      val streamReader = new FileBasedLogStreamReader(file)
      val stream = streamReader.listWithPosition(batchSize)
      consume(stream.map(_._3))

      //TODO: truncate file, length-stream.tail.apply(0)._2
      new RandomAccessFile(file, "rw").setLength(0)
    } while (stream.nonEmpty)
  }

  override def append(log: LogRecord): Unit = {
    val streamWriter = new FileBasedLogStreamWriter(file)
    streamWriter.append(log)
  }

  def flush(nodes: FileBasedNodeStore, rels: FileBasedRelationStore): Unit = {
    offer(500, (logs) => {
      val nodesToCreate = logs.filter(_.isInstanceOf[CreateNode]).map(_.asInstanceOf[CreateNode]).map(x => x.t.id -> x.t).toMap
      val nodeIdsToDelete = logs.filter(_.isInstanceOf[DeleteNode]).map(_.asInstanceOf[DeleteNode]).map(_.id).toSet
      val nodeIdsToCreate = nodesToCreate.keySet
      val combinedNodeIdsToCreate = nodeIdsToCreate.diff(nodeIdsToDelete)
      val combinedNodeIdsToDelete = nodeIdsToDelete.diff(nodeIdsToCreate)

      nodes.update(combinedNodeIdsToCreate.map(id => nodesToCreate(id)).iterator, combinedNodeIdsToDelete.iterator)
    })
  }
}

case class Node(id: Long, labels: String*) {

}

case class Relation(id: Long, from: Long, to: Long, labels: String*) {

}

class FileBasedNodeStore(seqFile: File) {
  val reader = new FileBasedSequenceReader[Node] {
    override def getFile: File = seqFile

    override def readObject(buf: ByteBuf): Node =
      Node(buf.readLong(), buf.readString().split(";").toSeq: _*)
  }

  def list(maxCount: Int = -1): Stream[Node] = reader.list(maxCount)

  val writer = new FileBasedSequenceWriter[Node] {
    override def getFile: File = seqFile

    override def writeObject(buf: ByteBuf, t: Node): Unit =
      buf.writeLong(t.id).writeString(t.labels.mkString(";"))
  }

  def update(nodesToCreate: Iterator[Node], nodeIdsToDeleted: Iterator[Long]) = {
    writer.append(nodesToCreate)
    nodeIdsToDeleted.foreach(writer.markDeleted(_))
  }
}

class FileBasedRelationStore(seqFile: File) {
  val reader = new FileBasedSequenceReader[Relation] {
    override def getFile: File = seqFile

    override def readObject(buf: ByteBuf): Relation = Relation(buf.readLong(), buf.readLong(), buf.readLong())
  }

  def list(maxCount: Int = -1): Stream[Relation] = reader.list(maxCount)
}