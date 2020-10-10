package cn.pandadb.pnode.store

import java.io._

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}

trait SequenceStore[Position, T] {
  def loadAll(): Stream[T]

  def loadAllWithPosition(): Stream[(Position, T)]

  def saveAll(ts: Stream[T])
}

trait Appendable[T] {
  def append(t: T): Unit = append(Some(t))

  def append(ts: Iterable[T]): Unit
}

trait Removable[Position] {
  def remove(pos: Position): Unit = remove(Some(pos))

  def remove(poss: Iterable[Position]): Unit
}

trait Closable {
  def close()
}

trait ObjectSerializer[T] {
  def readObject(buf: ByteBuf): T

  def writeObject(buf: ByteBuf, t: T)
}

trait ObjectBlockSerializer[T] {
  def readObjectBlock(dis: DataInputStream): (T, Int)

  def writeObjectBlock(buf: ByteBuf, t: T): Int
}

trait VariantSizedObjectBlockSerializer[T] extends ObjectBlockSerializer[T] {
  val objectSerializer: ObjectSerializer[T]

  //[length][object]
  def readObjectBlock(dis: DataInputStream): (T, Int) = {
    val len = dis.readInt()
    val bytes = new Array[Byte](len)
    dis.read(bytes)
    val buf = Unpooled.wrappedBuffer(bytes)
    val t = objectSerializer.readObject(buf)
    t -> (len + 4)
  }

  def writeObjectBlock(buf: ByteBuf, t: T): Int = {
    val obuf = Unpooled.buffer()
    objectSerializer.writeObject(obuf, t)
    val len = obuf.readableBytes()

    buf.writeInt(len) //length
    buf.writeBytes(obuf)
    len + 4
  }
}

trait FixedSizedObjectBlockSerializer[T] extends ObjectBlockSerializer[T] {
  val objectSerializer: ObjectSerializer[T]
  val fixedSize: Int
  lazy val bytes = new Array[Byte](fixedSize)

  def readObjectBlock(dis: DataInputStream): (T, Int) = {
    dis.readFully(bytes)
    val buf = Unpooled.wrappedBuffer(bytes)
    val t = objectSerializer.readObject(buf)
    t -> fixedSize
  }

  def writeObjectBlock(buf: ByteBuf, t: T): Int = {
    objectSerializer.writeObject(buf, t)
    fixedSize
  }
}

trait FileBasedSequenceStore[T] extends SequenceStore[Long, T] {
  val file: File

  val blockSerializer: ObjectBlockSerializer[T]

  override final def saveAll(ts: Stream[T]): Unit = {
    val appender = new FileOutputStream(file, false).getChannel
    val buf = PooledByteBufAllocator.DEFAULT.buffer()
    ts.foreach { t =>
      blockSerializer.writeObjectBlock(buf, t)
      if (buf.readableBytes() > 10240) {
        appender.write(buf.nioBuffer())
        buf.clear()
      }
    }

    if (buf.readableBytes() > 0)
      appender.write(buf.nioBuffer())

    appender.close()
  }

  override final def loadAll(): Stream[T] = {
    loadAllWithPosition().map(_._2)
  }

  override final def loadAllWithPosition(): Stream[(Long, T)] = {
    def createStream(dis: DataInputStream, offset: Long): Stream[(Long, T)] = {
      try {
        val t = blockSerializer.readObjectBlock(dis)
        Stream.cons(offset -> t._1, {
          createStream(dis, offset + t._2)
        })
      }
      catch {
        case _: EOFException => {
          dis.close()
          Stream.empty[(Long, T)]
        }
        case e => throw e
      }
    }

    val dis = new DataInputStream(new FileInputStream(file))
    createStream(dis, 0)
  }
}

trait AppendingFileBasedSequenceStore[T] extends FileBasedSequenceStore[T] with Appendable[T] with Closable {
  lazy val ptr = new FileOutputStream(file, true).getChannel

  def close(): Unit = {
    ptr.close()
  }

  def clear(): Unit = {
    ptr.truncate(0)
  }

  def append(ts: Iterable[T]) {
    val buf = Unpooled.buffer()
    ts.foreach(blockSerializer.writeObjectBlock(buf, _))
    ptr.write(buf.nioBuffer())
  }
}

trait RemovableFileBasedSequenceStore[T] extends FileBasedSequenceStore[T] with Appendable[T] with Removable[Long] with Closable with FixedSizedObjectBlockSerializer[T] {
  final val blockSerializer: ObjectBlockSerializer[T] = this

  lazy val ptr = new RandomAccessFile(file, "rw").getChannel

  def close(): Unit = {
    ptr.close()
  }

  def clear(): Unit = {
    ptr.truncate(0)
  }

  def append(ts: Iterable[T]) {
    val buf = Unpooled.buffer()
    ts.foreach(blockSerializer.writeObjectBlock(buf, _))
    ptr.position(ptr.size())
    ptr.write(buf.nioBuffer())
  }

  def remove(ps: Iterable[Long]): Unit = {
    val set = Set() ++ ps
    var total = ptr.size()
    set.foreach { pos =>
      if (pos < 0 || pos >= total || pos % fixedSize != 0)
        throw new InvalidPositionException(pos)

      ptr.position(pos)
      ptr.transferFrom(ptr, total - fixedSize, fixedSize)
      total -= fixedSize
    }

    ptr.truncate(total)
  }
}

class InvalidPositionException(pos: Long) extends RuntimeException {

}