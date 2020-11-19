package cn.pandadb.kernel.util

import java.io._
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.{ByteBuf, ByteBufAllocator, PooledByteBufAllocator, Unpooled}

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

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

  def saveAll(ts: Iterator[T]): Unit = {
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

  def loadAll(): Iterator[T] = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    new AbstractIterator[T] {
      //push None if reach EOF
      private val _buffered = ArrayBuffer[Option[T]]()

      //if empty, fetch one
      def fetchMoreIfEmpty: Unit = {
        if (_buffered.isEmpty) {
          try {
            _buffered += Some(blockSerializer.readObjectBlock(dis)._1)
          }
          catch {
            case _: EOFException => {
              dis.close()
              _buffered += None
            }

            case e => throw e
          }
        }
      }

      override def hasNext: Boolean = {
        fetchMoreIfEmpty
        _buffered.nonEmpty && _buffered.head.nonEmpty
      }

      override def next(): T = {
        fetchMoreIfEmpty
        _buffered.remove(0).get
      }
    }
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

  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  def append(ts: Iterable[T]): Unit = {
    val offset0 = ptr.size()
    var offset = offset0
    val buf = allocator.buffer()
    ts.foreach { t =>
      val buf0 = allocator.buffer()
      blockSerializer.writeObjectBlock(buf0, t)
      offset += buf0.readableBytes()
      buf.writeBytes(buf0)
      buf0.release()
    }

    ptr.write(buf.nioBuffer())
    buf.release()
  }
}