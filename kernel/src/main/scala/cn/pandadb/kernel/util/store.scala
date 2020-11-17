package cn.pandadb.kernel.util

import java.io._
import java.nio.ByteBuffer
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}

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

trait FileBasedArrayStore[T] {
  val file: File
  type Position = Long

  val blockSerializer: ObjectBlockSerializer[T]

  def saveAll(ts: Seq[T]): Unit = {
    val appender = new FileOutputStream(file, false).getChannel
    val buf = PooledByteBufAllocator.DEFAULT.buffer()
    ts.foreach { t =>
      blockSerializer.writeObjectBlock(buf, t)
      if (buf.readableBytes() > 10240) {
        appender.write(buf.nioBuffer())
        buf.clear()
      }
    }

    if (buf.readableBytes() > 0) {
      appender.write(buf.nioBuffer())
    }
    buf.release()
    appender.close()
  }

  def loadAll(): Seq[T] = {
    loadAllWithPosition().map(_._2)
  }

  def loadAllWithPosition(): Seq[(Position, T)] = {
    def createStream(dis: DataInputStream, offset: Position): Stream[(Long, T)] = {
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

trait AppendingFileBasedArrayStore[T] extends FileBasedArrayStore[T] {
  lazy val ptr = new FileOutputStream(file, true).getChannel

  def close(): Unit = {
    ptr.close()
  }

  def clear(): Unit = {
    ptr.truncate(0)
  }

  def append(ts: Iterable[T], posit: (T, Position) => Unit): Unit = {
    val offset0 = ptr.size()
    var offset = offset0
    val buf = Unpooled.buffer()
    ts.foreach { t =>
      val buf0 = Unpooled.buffer()
      blockSerializer.writeObjectBlock(buf0, t)
      posit(t, offset)
      offset += buf0.readableBytes()
      buf.writeBytes(buf0)
    }

    ptr.write(buf.nioBuffer())
    buf.release()
  }
}

trait RandomAccessibleFileBasedArrayStore[T] extends FileBasedArrayStore[T] with FixedSizedObjectBlockSerializer[T] {
  final val blockSerializer: ObjectBlockSerializer[T] = this
  lazy val ptr = new RandomAccessFile(file, "rw").getChannel

  def close(): Unit = {
    ptr.close()
  }

  def clear(): Unit = {
    ptr.truncate(0)
  }

  def append(ts: Iterable[T], posit: (T, Position) => Unit): Unit = {
    val buf = Unpooled.buffer()
    val offset0 = ptr.size()
    var offset = offset0
    ts.foreach { t =>
      val buf0 = Unpooled.buffer()
      blockSerializer.writeObjectBlock(buf0, t)
      posit(t, offset)
      offset += buf0.readableBytes()
      buf.writeBytes(buf0)
    }

    ptr.write(buf.nioBuffer(), offset0)
  }

  def remove(poss: Iterable[Position], posit: (T, Position) => Unit): Unit = {
    val set = Set() ++ poss
    var total = ptr.size()
    set.foreach { pos: Position =>
      if (pos < 0 || pos >= total || pos % fixedSize != 0)
        throw new InvalidPositionException(pos)

      total -= fixedSize
      val bytes = new Array[Byte](fixedSize)
      ptr.read(ByteBuffer.wrap(bytes), total)
      val (t, _) = readObjectBlock(new DataInputStream(new ByteArrayInputStream(bytes)))

      ptr.write(ByteBuffer.wrap(bytes), pos)
      posit(t, pos)
    }

    ptr.truncate(total)
  }

  def overwrite(ts: Iterable[(Position, T)], posit: (T, Position) => Unit): Unit = {
    val total = ptr.size()
    ts.foreach { x =>
      val (pos: Position, t: T) = x
      if (pos < 0 || pos >= total || pos % fixedSize != 0)
        throw new InvalidPositionException(pos)

      val buf = Unpooled.buffer()
      writeObjectBlock(buf, t)

      posit(t, pos)
      ptr.write(buf.nioBuffer(), pos)
    }

    ptr.truncate(total)
  }
}

class InvalidPositionException(pos: Long) extends RuntimeException {

}