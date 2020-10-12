package cn.pandadb.pnode.store

import java.io._
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}

trait SequenceStore[T, Position] {
  def loadAll(): Stream[T]

  def loadAllWithPosition(): Stream[(Position, T)]

  def saveAll(ts: Stream[T])
}

trait Appendable[T, Position] {
  def append(t: T): Position = append(Some(t)).head

  def append(ts: Iterable[T]): Iterable[Position]
}

trait RandomAccessible[T, Position] extends Appendable[T, Position] {
  def remove(pos: Position, move: (Position, Position) => Unit): Unit = remove(Some(pos), move)

  def remove(poss: Iterable[Position], move: (Position, Position) => Unit): Unit

  def replace(pos: Position, t: T): Unit = replace(Some(pos -> t))

  def replace(ts: Iterable[(Position, T)]): Unit
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
    val buf0 = Unpooled.buffer()
    objectSerializer.writeObject(buf0, t)
    val len = buf0.readableBytes()

    buf.writeInt(len) //length
    buf.writeBytes(buf0)
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

trait FileBasedSequenceStore[T] extends SequenceStore[T, Long] {
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

trait AppendingFileBasedSequenceStore[T] extends FileBasedSequenceStore[T] with Appendable[T, Long] with Closable {
  lazy val ptr = new FileOutputStream(file, true).getChannel

  override def close(): Unit = {
    ptr.close()
  }

  def clear(): Unit = {
    ptr.truncate(0)
  }

  override def append(ts: Iterable[T]): Iterable[Long] = {
    val offset0 = ptr.size()
    var offset = offset0
    val buf = Unpooled.buffer()
    val lens = ts.map { t =>
      val buf0 = Unpooled.buffer()
      blockSerializer.writeObjectBlock(buf0, t)
      offset += buf0.readableBytes()
      buf.writeBytes(buf0)
      offset
    }

    ptr.write(buf.nioBuffer())
    lens
  }
}

trait RandomAccessibleFileBasedSequenceStore[T] extends FileBasedSequenceStore[T] with Appendable[T, Long]
  with RandomAccessible[T, Long] with Closable with FixedSizedObjectBlockSerializer[T] {
  final val blockSerializer: ObjectBlockSerializer[T] = this

  lazy val ptr = new RandomAccessFile(file, "rw").getChannel

  override def close(): Unit = {
    ptr.close()
  }

  def clear(): Unit = {
    ptr.truncate(0)
  }

  override def append(ts: Iterable[T]): Iterable[Long] = {
    val buf = Unpooled.buffer()
    val offset0 = ptr.size()
    var offset = offset0
    val lens = ts.map { t =>
      val buf0 = Unpooled.buffer()
      blockSerializer.writeObjectBlock(buf0, t)
      offset += buf0.readableBytes()
      buf.writeBytes(buf0)
      offset
    }

    ptr.position(offset0)
    ptr.write(buf.nioBuffer())

    lens
  }

  override def remove(poss: Iterable[Long], move: (Long, Long) => Unit): Unit = {
    val set = Set() ++ poss
    var total = ptr.size()
    set.foreach { pos =>
      if (pos < 0 || pos >= total || pos % fixedSize != 0)
        throw new InvalidPositionException(pos)

      total -= fixedSize
      val buf = ByteBuffer.allocate(fixedSize)
      ptr.read(buf, total)

      ptr.position(pos)
      ptr.write(buf, pos)
      move(total, pos)
    }

    ptr.truncate(total)
  }

  override def replace(ts: Iterable[(Long, T)]): Unit = {
    val total = ptr.size()
    ts.foreach { x =>
      val (pos: Long, t: T) = x
      if (pos < 0 || pos >= total || pos % fixedSize != 0)
        throw new InvalidPositionException(pos)

      val buf = Unpooled.buffer()
      writeObjectBlock(buf, t)
      ptr.position(pos)
      ptr.write(buf.nioBuffer(), pos)
    }

    ptr.truncate(total)
  }
}

class InvalidPositionException(pos: Long) extends RuntimeException {

}