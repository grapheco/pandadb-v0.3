package cn.pandadb.pnode.store

import java.io._
import io.netty.buffer.{ByteBuf, Unpooled}

trait LogRecord {
}

trait SequenceStore[T] {
  def list(): Stream[T]

  def save(ts: Stream[T])

  def append(t: T)

  def append(ts: Iterable[T])

  def clear()

  def close()
}

/**
 * [llll][...]
 * l-length of block (excluding d & l)
 */
trait FileBasedSequenceStore[T] extends SequenceStore[T] {
  def getFile: File

  def readObject(buf: ByteBuf): T

  def close(): Unit = {
    appender.close()
  }

  private def createStream(dis: DataInputStream): Stream[T] = {
    try {
      val len = dis.readInt()
      val bytes = new Array[Byte](len)
      dis.read(bytes)
      val buf = Unpooled.wrappedBuffer(bytes)
      val t = readObject(buf)
      Stream.cons(t, {
        createStream(dis)
      })
    }
    catch {
      case _: EOFException => {
        dis.close()
        Stream.empty[T]
      }
      case e => throw e
    }
  }

  def list(): Stream[T] = {
    val dis = new DataInputStream(new FileInputStream(getFile))
    createStream(dis)
  }

  def clear(): Unit = {
    appender.getChannel.truncate(0)
  }

  def writeObject(buf: ByteBuf, t: T)

  val OBJECT_BLOCK_BUF = new Array[Byte](1024)

  protected def writeObjectBlock(buf: ByteBuf, t: T): Unit = {
    val buf0 = Unpooled.wrappedBuffer(OBJECT_BLOCK_BUF)
    buf0.resetWriterIndex()
    writeObject(buf0, t)
    val len = buf0.readableBytes()

    buf.writeInt(len) //length
    buf.writeBytes(buf0)
  }

  def save(ts: Stream[T]): Unit = {
    val appender = new FileOutputStream(getFile, false).getChannel
    val buf = Unpooled.buffer()
    ts.foreach { t =>
      writeObjectBlock(buf, t)
      if (buf.readableBytes() > 10240) {
        appender.write(buf.nioBuffer())
        buf.clear()
      }
    }

    if (buf.readableBytes() > 0)
      appender.write(buf.nioBuffer())

    appender.close()
  }

  lazy val appender = new FileOutputStream(getFile, true)

  def append(t: T) {
    append(Some(t))
  }

  def append(ts: Iterable[T]) {
    val buf = Unpooled.buffer()
    ts.foreach(writeObjectBlock(buf, _))
    appender.write(buf.array().slice(0, buf.readableBytes()))
    appender.flush()
  }
}