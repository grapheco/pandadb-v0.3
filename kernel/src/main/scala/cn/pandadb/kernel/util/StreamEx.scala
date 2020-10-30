package cn.pandadb.kernel.util

import java.io.{DataInputStream, DataOutputStream}

import io.netty.buffer.ByteBuf

class ByteBufEx(buf: ByteBuf) {
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
