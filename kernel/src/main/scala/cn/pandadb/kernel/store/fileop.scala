//package cn.pandadb.kernel.store
//
//import java.io._
//import java.nio.ByteBuffer
//import io.netty.buffer.{ByteBuf, ByteBufAllocator, PooledByteBufAllocator, Unpooled}
//import scala.collection.AbstractIterator
//import scala.collection.mutable.ArrayBuffer
//
//trait FileBasedArrayStore[T] extends ArrayStore[T] {
//  val file: File
//
//  val blockSerializer: ObjectBlockSerializer[T]
//
//  lazy val ptr = new FileOutputStream(file, true).getChannel
//
//  override def close(): Unit = {
//    ptr.close()
//  }
//
//  override def clear(): Unit = {
//    ptr.truncate(0)
//  }
//
//  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT
//
//  override def append(ts: Iterable[T], updatePositions: Iterable[(T, Long)] => Unit): Unit = {
//    if (ts.nonEmpty) {
//      val offset0 = ptr.position()
//      val results = ArrayBuffer[(T, Long)]()
//      var offset = offset0
//      val buf = allocator.buffer()
//      ts.foreach { t =>
//        val buf0 = allocator.buffer()
//        blockSerializer.writeObjectBlock(buf0, t)
//        results += t -> offset
//        offset += buf0.readableBytes()
//        buf.writeBytes(buf0)
//        buf0.release()
//      }
//
//      ptr.write(buf.nioBuffer())
//      updatePositions(results)
//
//      buf.release()
//    }
//  }
//
//  val MAX_BUFFER_SIZE: Int = 10240
//
//  override def saveAll(ts: Iterator[T], updatePositions: Iterable[(T, Long)] => Unit): Unit = {
//    val results = ArrayBuffer[(T, Long)]()
//    val appender = new FileOutputStream(file, false).getChannel
//    val buf = allocator.buffer()
//
//    def flush(): Unit = {
//      appender.write(buf.nioBuffer())
//      updatePositions(results)
//      buf.clear()
//      results.clear()
//    }
//
//    ts.foreach { t =>
//      results += t -> buf.readableBytes()
//      blockSerializer.writeObjectBlock(buf, t)
//
//      if (buf.readableBytes() > MAX_BUFFER_SIZE) {
//        flush()
//      }
//    }
//
//    if (results.nonEmpty) {
//      flush()
//    }
//
//    buf.release()
//    appender.close()
//  }
//
//  override def loadAll(): Iterator[T] = {
//    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
//    new AbstractIterator[T] {
//      //push None if reach EOF
//      private val _buffered = ArrayBuffer[Option[T]]()
//
//      //if empty, fetch one more
//      def fetchMoreIfEmpty: Unit = {
//        if (_buffered.isEmpty) {
//          try {
//            _buffered += Some(blockSerializer.readObjectBlock(dis)._1)
//          }
//          catch {
//            case _: EOFException => {
//              dis.close()
//              _buffered += None
//            }
//
//            case e => throw e
//          }
//        }
//      }
//
//      override def hasNext: Boolean = {
//        fetchMoreIfEmpty
//        _buffered.nonEmpty && _buffered.head.nonEmpty
//      }
//
//      override def next(): T = {
//        fetchMoreIfEmpty
//        _buffered.remove(0).get
//      }
//    }
//  }
//}
//
//trait FileBasedRemovableArrayStore[T] extends RemovableArrayStore[T] {
//  val self = this
//  val file: File
//
//  val blockSerializer: ObjectBlockSerializer[T]
//
//  private val _store = new FileBasedArrayStore[(Byte, T)] {
//    override lazy val file: File = self.file
//    override val blockSerializer: ObjectBlockSerializer[(Byte, T)] = new ObjectBlockSerializer[(Byte, T)] {
//      override def readObjectBlock(dis: DataInputStream): ((Byte, T), Int) = {
//        val mark = dis.readByte()
//        val (t, len) = self.blockSerializer.readObjectBlock(dis)
//        (mark, t) -> (1 + len)
//      }
//
//      override def writeObjectBlock(buf: ByteBuf, t: (Byte, T)): Int = {
//        buf.writeByte(t._1)
//        self.blockSerializer.writeObjectBlock(buf, t._2)
//      }
//    }
//  }
//
//  lazy val ptr = _store.ptr
//
//  override def markDeleted(pos: Long) = {
//    ptr.write(ByteBuffer.wrap(Array[Byte](0)), pos)
//  }
//
//  override def append(ts: Iterable[T], updatePositions: Iterable[(T, Long)] => Unit): Unit = {
//    _store.append(ts.map(1.toByte -> _),
//      (ts: Iterable[((Byte, T), Long)]) => updatePositions(ts.map(x => x._1._2 -> x._2)))
//  }
//
//  override def saveAll(ts: Iterator[T], updatePositions: Iterable[(T, Long)] => Unit): Unit = {
//    _store.saveAll(ts.map(1.toByte -> _),
//      (ts: Iterable[((Byte, T), Long)]) => updatePositions(ts.map(x => x._1._2 -> x._2)))
//  }
//
//  def close(): Unit = _store.close()
//
//  def clear(): Unit = _store.clear()
//
//  def loadAll(): Iterator[T] = _store.loadAll().filter(_._1 != 0).map(_._2)
//}
//
//class PositionMapFile(val mapFile: File) {
//  private val _store = new FileBasedPositionMappedArrayStore[Long]() {
//    override val file: File = mapFile
//    override val objectSerializer: ObjectSerializer[Long] = new ObjectSerializer[Long] {
//      override def readObject(buf: ByteBuf): Long = buf.readLong()
//
//      override def writeObject(buf: ByteBuf, t: Long): Unit = buf.writeLong(t)
//    }
//
//    override val fixedSize: Int = 8
//  }
//
//  def positionOf(id: Long): Long = _store.read(id).get
//
//  def update(ts: Iterator[(Long, Long)]): Unit = _store.update(ts)
//
//  def close(): Unit = _store.close()
//}
//
////position-mapped
////fixed-sized
//trait FileBasedPositionMappedArrayStore[T] {
//  val file: File
//  val objectSerializer: ObjectSerializer[T]
//  val fixedSize: Int
//  lazy val ptr = new RandomAccessFile(file, "rw").getChannel
//  lazy val bytes = new Array[Byte](fixedSize)
//
//  protected def positionOf(id: Long): Long = fixedSize * id
//
//  private def readObjectBlock(position: Long): Option[T] = {
//    val bbuf = ByteBuffer.wrap(bytes)
//    if (ptr.read(bbuf, position) == -1) {
//      None
//    }
//    else {
//      val buf = Unpooled.wrappedBuffer(bytes)
//      Some(objectSerializer.readObject(buf))
//    }
//  }
//
//  private def writeObjectBlock(buf: ByteBuf, t: T): Unit = {
//    objectSerializer.writeObject(buf, t)
//  }
//
//  val allocator: ByteBufAllocator = ByteBufAllocator.DEFAULT
//
//  def read(id: Long): Option[T] = {
//    readObjectBlock(positionOf(id))
//  }
//
//  lazy val MAX_BUFFER_SIZE = 10240 / fixedSize
//
//  def update(ts: Iterator[(Long, T)]) = {
//    val buffered = ArrayBuffer[T]()
//    var (start, end) = (-1L, -2L)
//
//    def flush() {
//      if (buffered.nonEmpty) {
//        ptr.position(positionOf(start))
//        val buf = allocator.buffer()
//        buffered.foreach {
//          writeObjectBlock(buf, _)
//        }
//        ptr.write(buf.nioBuffer())
//        buf.release()
//        buffered.clear()
//      }
//
//      start = -1
//      end = -2
//    }
//
//    ts.foreach { it =>
//      val (id, t) = it
//
//      if (id == start - 1) {
//        start = id
//        buffered.insert(0, t)
//      }
//      else if (id == end + 1) {
//        end = id
//        buffered.append(t)
//      }
//      else {
//        //not continual
//        flush()
//        start = id
//        end = id
//        buffered.append(t)
//      }
//
//      if (buffered.size > MAX_BUFFER_SIZE) {
//        flush()
//      }
//    }
//
//    flush()
//  }
//
//  def close(): Unit = {
//    ptr.close()
//  }
//}