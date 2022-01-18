package org.grapheco.pandadb.kernel.util

import io.netty.buffer.ByteBuf

class ByteBufEx(buf: ByteBuf) {
  def readInt40(): Long = buf.readByte() << 32 | buf.readInt()

  def writeInt40(v: Long): ByteBuf = {
    buf.writeByte((v >> 32).toByte)
    buf.writeInt((v & 0xFFFFFFFF).toInt)
    buf
  }

  def readString(): String = {
    val len = buf.readInt()
    readString(len)
  }

  def readRemainingAsString(): String = {
    val len = buf.readableBytes()
    readString(len)
  }

  private def readString(len: Int): String = {
    val bs = new Array[Byte](len)
    buf.readBytes(bs)
    new String(bs, "utf-8")
  }

  def writeString(s: String, includesLengthAhead: Boolean = true) = {
    if (includesLengthAhead)
      buf.writeInt(s.length)
    buf.writeBytes(s.getBytes("utf-8"))
  }
}

object StreamExLike {
  implicit def ex(buf: ByteBuf): ByteBufEx = new ByteBufEx(buf)
}
