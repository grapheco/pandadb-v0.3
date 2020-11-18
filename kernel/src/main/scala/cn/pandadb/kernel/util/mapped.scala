package cn.pandadb.kernel.util

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.{ByteBuf, Unpooled}

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

trait PositionMappedArrayStore[T] {
  def loadAll(): Iterator[(Long, T)]

  def markDeleted(id: Long)

  def update(id: Long, t: T)

  def saveAll(ts: Iterator[(Long, T)])
}

trait FileBasedPositionMappedArrayStore[T] extends PositionMappedArrayStore[T] {
  val file: File
  val objectSerializer: ObjectSerializer[T]
  val fixedSize: Int
  val blockSerializer = this
  lazy val ptr = new RandomAccessFile(file, "rw").getChannel
  lazy val bytes = new Array[Byte](1 + fixedSize)

  protected def positionOf(id: Long): Long = (1 + fixedSize) * id

  def readObjectBlock(dis: DataInputStream): Option[T] = {
    dis.readFully(bytes)
    val buf = Unpooled.wrappedBuffer(bytes)
    val flag = buf.readByte()
    if (flag == 0)
      None
    else
      Some(objectSerializer.readObject(buf))
  }

  def writeObjectBlock(buf: ByteBuf, t: T): Unit = {
    buf.writeByte(1)
    objectSerializer.writeObject(buf, t)
  }

  override def loadAll(): Iterator[(Long, T)] = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    new AbstractIterator[(Long, T)] {
      private val idx = new AtomicLong()
      //push None if reach EOF
      private val _buffered = ArrayBuffer[Option[(Long, T)]]()

      //if empty, fetch one
      def fetchMoreIfEmpty: Unit = {
        if (_buffered.isEmpty) {
          try {
            var opt: Option[(Long, T)] = None
            do {
              opt = blockSerializer.readObjectBlock(dis).map(t => idx.incrementAndGet() -> t)
            } while (opt.isEmpty)

            _buffered += opt
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

      override def next(): (Long, T) = {
        fetchMoreIfEmpty
        _buffered.remove(0).get
      }
    }
  }

  override def markDeleted(id: Long) = {
    ptr.position(positionOf(id))
    ptr.write(ByteBuffer.wrap(Array[Byte](0)))
  }

  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT

  override def update(id: Long, t: T) = {
    ptr.position(positionOf(id))
    val buf = allocator.buffer()
    blockSerializer.writeObjectBlock(buf, t)
    ptr.write(buf.nioBuffer())
    buf.release()
  }

  override def saveAll(ts: Iterator[(Long, T)]) = {
    ptr.truncate(0)
    ts.foreach(x => update(x._1, x._2))
  }

  def close(): Unit = {
    ptr.close()
  }
}