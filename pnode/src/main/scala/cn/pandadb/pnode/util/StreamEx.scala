package cn.pandadb.pnode.util

import java.io.{DataInputStream, DataOutputStream}

import io.netty.buffer.ByteBuf

class DataInputStreamEx(dis: DataInputStream) {
  def readString(): String = {
    val len = dis.readInt()
    val bs = new Array[Byte](len)
    dis.readFully(bs)
    new String(bs, "utf-8")
  }
}

class ByteBufEx(buf: ByteBuf) {
  def readString(): String = {
    val len = buf.readInt()
    val bs = new Array[Byte](len)
    buf.readBytes(bs)
    new String(bs, "utf-8")
  }

  def writeString(s:String)={
    buf.writeInt(s.length)
    buf.writeBytes(s.getBytes("utf-8"))
  }
}

class DataOutputStreamEx(dos: DataOutputStream) {
  def writeString(s: String) = {
    dos.writeInt(s.length)
    dos.write(s.getBytes("utf-8"))
  }
}

object StreamExLike {
  implicit def ex(dis: DataInputStream): DataInputStreamEx = new DataInputStreamEx(dis)

  implicit def ex(buf: ByteBuf): ByteBufEx = new ByteBufEx(buf)

  implicit def ex(dos: DataOutputStream): DataOutputStreamEx = new DataOutputStreamEx(dos)
}
