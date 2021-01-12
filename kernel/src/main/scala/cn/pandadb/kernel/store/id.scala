//package cn.pandadb.kernel.store
//
//import java.io.{DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream}
//import java.util.concurrent.atomic.AtomicLong
//import org.apache.logging.log4j.scala.Logging
//
//class FileBasedIdGen(file: File, sequenceSize: Int) extends Logging {
//
//  private val id: AtomicLong = {
//    if (file.length() == 0) {
//      new AtomicLong(0)
//    }
//    else {
//      val fis = new DataInputStream(new FileInputStream(file))
//      val current = fis.readLong()
//      fis.close()
//
//      new AtomicLong(current)
//    }
//  }
//
//  private val bound = new AtomicLong(0)
//
//  private def slideDown(current: Long): Unit = {
//    val end = current + sequenceSize - 1
//    writeId(end)
//    bound.set(end)
//  }
//
//  private def slideDownIfNeeded(nid: Long): Unit = {
//    if (nid > bound.get()) {
//      slideDown(nid)
//    }
//  }
//
//  def currentId() = id.get()
//
//  def update(newId: Long): Unit = {
//    if (newId < currentId()) {
//      throw new LowerIdSetException(newId)
//    }
//
//    id.set(newId)
//    slideDownIfNeeded(newId)
//  }
//
//  def nextId(): Long = {
//    val nid = id.incrementAndGet()
//    //all ids consumed
//    slideDownIfNeeded(nid)
//    nid
//  }
//
//  //save current id
//  def flush(): Unit = {
//    writeId(id.get())
//  }
//
//  private def writeId(num: Long): Unit = {
//    val fos = new DataOutputStream(new FileOutputStream(file))
//    fos.writeLong(num)
//    fos.close()
//
//    logger.trace(s"current: ${id.get()}, written: $num")
//  }
//}
//
//class LowerIdSetException(id: Long) extends RuntimeException(s"lower id set: $id") {
//
//}
