package cn.pandadb.kernel.util.serializer

import cn.pandadb.kernel.kv.db.KeyValueIterator

import scala.collection.mutable.ArrayBuffer
/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 5:39 下午 2021/9/25
 * @Modified By:
 */

object PandaIteratorForDeSerializer {
  private def _transferIter(kvIter: KeyValueIterator): Iterator[Array[Byte]] = {
    kvIter.seekToFirst()
    new Iterator[Array[Byte]]() {
      override def hasNext: Boolean = kvIter.isValid

      override def next(): Array[Byte] = {
        val bytes = kvIter.value()
        kvIter.next()
        bytes
      }
    }
  }

}

class PandaIteratorForDeSerializer[T](sourceIter: Iterator[Array[Byte]], stepLength: Int = 1000000, function: (Array[Array[Byte]], Int) => Array[T]) extends Iterator[T] {

  def this(kvIter: KeyValueIterator, stepLength: Int, function: (Array[Array[Byte]], Int) => Array[T]) {
    this(PandaIteratorForDeSerializer._transferIter(kvIter), stepLength, function)
  }

  private def _getBatchSource: Array[Array[Byte]] = {
    var n = stepLength
    val buf: ArrayBuffer[Array[Byte]] = new ArrayBuffer[Array[Byte]]()

    while (n > 0 && sourceIter.hasNext) {
      buf.append(sourceIter.next())
      n -= 1
    }
    buf.toArray
  }

  private def _prepareIter: Iterator[T] = function(_getBatchSource, math.max(Runtime.getRuntime.availableProcessors()/4, 2)).toIterator
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
