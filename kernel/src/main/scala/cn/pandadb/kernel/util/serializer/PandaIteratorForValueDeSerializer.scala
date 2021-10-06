package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.kv.db.KeyValueIterator

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 5:39 下午 2021/9/25
 * @Modified By:
 */

object PandaIteratorUtil {
  def getVIter(kvIter: KeyValueIterator): Iterator[Array[Byte]] = {
    kvIter.seekToFirst()
    new Iterator[Array[Byte]]() {
      override def hasNext: Boolean = kvIter.isValid

      override def next(): Array[Byte] = {
        val value = kvIter.value()
        kvIter.next()
        value
      }
    }
  }

  def getKVIter(kvIter: KeyValueIterator): Iterator[(Array[Byte], Array[Byte])] = {
    kvIter.seekToFirst()
    new Iterator[(Array[Byte], Array[Byte])]() {
      override def hasNext: Boolean = kvIter.isValid

      override def next(): (Array[Byte], Array[Byte]) = {
        val key = kvIter.key()
        val value = kvIter.value()
        kvIter.next()
        (key, value)
      }
    }
  }

}

class PandaIteratorForKeyValueDeserializer[T1:ClassTag, T2:ClassTag](sourceIter: Iterator[(Array[Byte], Array[Byte])],
                                                   stepLength: Int = BaseSerializer.generalStepLength,
                                                   deserializeKey: Array[Byte] => T1,
                                                                     deserializeValue: Array[Byte]=> T2) extends Iterator[(T1, T2)] {
  def this(kvIter: KeyValueIterator, stepLength: Int, deserializeKey: Array[Byte] => T1, deserializeValue: Array[Byte]=> T2) {
    this(PandaIteratorUtil.getKVIter(kvIter), stepLength, deserializeKey, deserializeValue)
  }

  val resourcePool: LinkedBlockingQueue[Array[(Array[Byte], Array[Byte])]] = new LinkedBlockingQueue[Array[(Array[Byte], Array[Byte])]] (2)
  private val _getBatchSource = new Runnable {
    override def run(): Unit = {
      var n = stepLength
      val buf: ArrayBuffer[(Array[Byte], Array[Byte])] = new ArrayBuffer[(Array[Byte], Array[Byte])]()

      while (n > 0 && sourceIter.hasNext) {
        buf.append(sourceIter.next())
        n -= 1
      }
      resourcePool.put(buf.toArray)
    }
  }

  val service = Executors.newSingleThreadScheduledExecutor()
  service.scheduleWithFixedDelay(_getBatchSource, 0, 1, TimeUnit.MILLISECONDS)

  private def _prepareIter: Iterator[(T1, T2)] =
    BaseSerializer.batchDeserialize[T1, T2](resourcePool.take(), deserializeKey, deserializeValue).toIterator
  var iter: Iterator[(T1, T2)] = _prepareIter

  override def hasNext: Boolean = sourceIter.hasNext | iter.hasNext

  override def next(): (T1, T2) = {
    if(iter.hasNext) iter.next()
    else {
      iter = _prepareIter
      iter.next()
    }
  }

}


class PandaIteratorForValueDeSerializer[T: ClassTag](sourceIter: Iterator[Array[Byte]],
                                                     stepLength: Int = BaseSerializer.generalStepLength,
                                                     deserializeValue: Array[Byte] => T) extends Iterator[T] {

  def this(kvIter: KeyValueIterator, stepLength: Int, function: Array[Byte] => T) {
    this(PandaIteratorUtil.getVIter(kvIter), stepLength, function)
  }

  val resourcePool: LinkedBlockingQueue[Array[Array[Byte]]] = new LinkedBlockingQueue[Array[Array[Byte]]] (2)
  private val _getBatchSource = new Runnable {
    override def run(): Unit = {
      var n = stepLength
      val buf: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]]()

      while (n > 0 && sourceIter.hasNext) {
        buf.append(sourceIter.next())
        n -= 1
      }
      resourcePool.put(buf.toArray)
    }
  }

  val service = Executors.newSingleThreadScheduledExecutor()
  service.scheduleWithFixedDelay(_getBatchSource, 0, 1, TimeUnit.MILLISECONDS)

  private def _prepareIter: Iterator[T] =
    BaseSerializer.batchDeserialize[T](resourcePool.take(), deserializeValue).toIterator
  var iter: Iterator[T] = _prepareIter

  override def hasNext: Boolean = sourceIter.hasNext | iter.hasNext

  override def next(): T = {
    if(iter.hasNext) iter.next()
    else {
      iter = _prepareIter
      iter.next()
    }
  }
}
