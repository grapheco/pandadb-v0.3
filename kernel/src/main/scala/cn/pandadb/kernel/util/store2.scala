package cn.pandadb.kernel.util

import java.io._
import java.nio.ByteBuffer
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.{ByteBuf, Unpooled}

trait PositionMappedArrayStore[T] {
  def loadAll(): Seq[(Long, T)]

  def markDeleted(id: Long)

  def update(id: Long, t: T)

  def saveAll(ts: Seq[(Long, T)])
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

  override def loadAll(): Seq[(Long, T)] = {
    def createStream(dis: DataInputStream, id: Long): Stream[(Long, T)] = {
      try {
        val opt = blockSerializer.readObjectBlock(dis)
        if (opt.nonEmpty)
          Stream.cons(id -> opt.get, {
            createStream(dis, id + 1)
          })
        else
          createStream(dis, id + 1)
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

  override def saveAll(ts: Seq[(Long, T)]) = {
    ts.foreach(x => update(x._1, x._2))
  }

  def close(): Unit = {
    ptr.close()
  }
}
