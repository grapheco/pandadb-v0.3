package cn.pandadb.kernel.store

import java.io._

import io.netty.buffer.{ByteBuf, Unpooled}

trait ObjectSerializer[T] {
  def readObject(buf: ByteBuf): T

  def writeObject(buf: ByteBuf, t: T)
}

trait ObjectBlockSerializer[T] {
  /**
   * @return read object & length
   */
  def readObjectBlock(dis: DataInputStream): (T, Int)

  def writeObjectBlock(buf: ByteBuf, t: T): Int
}

trait LengthSerializer {
  def readLength(dis: DataInputStream): Int

  def writeLength(buf: ByteBuf, len: Int)

  val maxValue: Int
}

object LengthSerializer {
  val INT = new LengthSerializer() {
    override def readLength(dis: DataInputStream): Int = dis.readInt()

    override def writeLength(buf: ByteBuf, len: Int): Unit = buf.writeInt(len)

    override val maxValue: Int = Int.MaxValue
  }

  val BYTE = new LengthSerializer() {
    override def readLength(dis: DataInputStream): Int = dis.readByte()

    override def writeLength(buf: ByteBuf, len: Int): Unit = buf.writeByte(len)

    override val maxValue: Int = Byte.MaxValue
  }
}

trait VariantSizedObjectBlockSerializer[T] extends ObjectBlockSerializer[T] {
  val objectSerializer: ObjectSerializer[T]
  val lengthSerializer: LengthSerializer = LengthSerializer.INT

  //[length][object]
  def readObjectBlock(dis: DataInputStream): (T, Int) = {
    val len = lengthSerializer.readLength(dis)
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
    if (len > lengthSerializer.maxValue) {
      throw new TooLargeBlockException(len, lengthSerializer.maxValue)
    }
    lengthSerializer.writeLength(buf, len)
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

trait ArrayStore[T] {
  def close(): Unit

  def clear(): Unit

  def append(ts: Iterable[T], updatePositions: Iterable[(T, Long)] => Unit): Unit

  def saveAll(ts: Iterator[T], updatePositions: Iterable[(T, Long)] => Unit): Unit

  def loadAll(): Iterator[T]
}

trait RemovableArrayStore[T] extends ArrayStore[T] {
  def markDeleted(pos: Long): Unit
}

class TooLargeBlockException(actual: Int, maxValue: Int) extends RuntimeException