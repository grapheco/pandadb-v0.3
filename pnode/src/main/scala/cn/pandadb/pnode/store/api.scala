package cn.pandadb.pnode.store

import java.io._

import io.netty.buffer.{ByteBuf, PooledByteBufAllocator, Unpooled}

trait LogRecord {
}

trait SequenceStore[Position, T] {
  def load(): Stream[T]

  def loadWithPosition(): Stream[(Position, T)]

  def save(ts: Stream[T])

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

trait ObjectBlockSerializationStrategy[T] {
  def readObjectBlock(dis: DataInputStream): (T, Int)

  def writeObjectBlock(buf: ByteBuf, t: T): Int
}

trait VariantSizedObjectBlockSerializationStrategy[T] extends ObjectBlockSerializationStrategy[T] {
  val orw: ObjectSerializer[T]

  def readObjectBlock(dis: DataInputStream): (T, Int) = {
    val len = dis.readInt()
    val bytes = new Array[Byte](len)
    dis.read(bytes)
    val buf = Unpooled.wrappedBuffer(bytes)
    val t = orw.readObject(buf)
    t -> (len + 4)
  }

  val OBJECT_BLOCK_BUF = new Array[Byte](1024)

  def writeObjectBlock(buf: ByteBuf, t: T): Int = {
    val buf0 = Unpooled.wrappedBuffer(OBJECT_BLOCK_BUF)
    orw.writeObject(buf0, t)
    val len = buf0.readableBytes()

    buf.writeInt(len) //length
    buf.writeBytes(buf0)
    len + 4
  }
}

trait FixedSizedObjectBlockSerializationStrategy[T] extends ObjectBlockSerializationStrategy[T] {
  val orw: ObjectSerializer[T]
  val fixedSize: Int

  def readObjectBlock(dis: DataInputStream): (T, Int) = {
    val bytes = new Array[Byte](fixedSize)
    dis.readFully(bytes)
    val buf = Unpooled.wrappedBuffer(bytes)
    val t = orw.readObject(buf)
    t -> fixedSize
  }

  def writeObjectBlock(buf: ByteBuf, t: T): Int = {
    orw.writeObject(buf, t)
    fixedSize
  }
}

trait FileBasedSequenceStore[T] {
  val file: File

  val obss: ObjectBlockSerializationStrategy[T]

  def save(ts: Stream[T]): Unit = {
    val appender = new FileOutputStream(file, false).getChannel
    val buf = PooledByteBufAllocator.DEFAULT.buffer()
    ts.foreach { t =>
      obss.writeObjectBlock(buf, t)
      if (buf.readableBytes() > 10240) {
        appender.write(buf.nioBuffer())
        buf.clear()
      }
    }

    if (buf.readableBytes() > 0)
      appender.write(buf.nioBuffer())

    appender.close()
  }

  def load(): Stream[T] = {
    loadWithPosition().map(_._2)
  }

  def loadWithPosition(): Stream[(Long, T)] = {
    def createStream(dis: DataInputStream, offset: Long): Stream[(Long, T)] = {
      try {
        val t = obss.readObjectBlock(dis)
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
  lazy val appender = new FileOutputStream(file, true)

  def close(): Unit = {
    appender.close()
  }

  def clear(): Unit = {
    appender.getChannel.truncate(0)
  }

  def append(ts: Iterable[T]) {
    val buf = Unpooled.buffer()
    ts.foreach(obss.writeObjectBlock(buf, _))
    appender.write(buf.array().slice(0, buf.readableBytes()))
    appender.flush()
  }
}

trait RemovableFileBasedSequenceStore[T] extends FileBasedSequenceStore[T] with Appendable[T] with Removable[Long] with Closable with FixedSizedObjectBlockSerializationStrategy[T] {
  final val obss: ObjectBlockSerializationStrategy[T] = this

  lazy val appender = new RandomAccessFile(file, "rw").getChannel

  def close(): Unit = {
    appender.close()
  }

  def clear(): Unit = {
    appender.truncate(0)
  }

  def append(ts: Iterable[T]) {
    val buf = Unpooled.buffer()
    ts.foreach(obss.writeObjectBlock(buf, _))
    appender.position(appender.size())
    appender.write(buf.nioBuffer())
  }

  def remove(ps: Iterable[Long]): Unit = {
    val set = Set() ++ ps
    var total = appender.size()
    set.foreach { pos =>
      if (pos < 0 || pos >= total || pos % fixedSize != 0)
        throw new InvalidPositionException(pos)

      appender.position(pos)
      appender.transferFrom(appender, total - fixedSize, fixedSize)
      total -= fixedSize
    }

    appender.truncate(total)
  }
}

class InvalidPositionException(pos: Long) extends RuntimeException {

}