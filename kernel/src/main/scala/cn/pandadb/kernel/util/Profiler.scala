package cn.pandadb.kernel.util

import com.typesafe.scalalogging.LazyLogging

object Profiler extends LazyLogging {
  def timing[T](f: => T): T = {
    val t1 = System.currentTimeMillis()
    val t = f
    val t2 = System.currentTimeMillis()
    println(s"time cost: ${t2 - t1} ms")
    t
  }

  def timingByMicroSec[T](f: => T): T = {
    val t1 = System.nanoTime()
    val t = f
    val t2 = System.nanoTime()
    println(s"time cost: ${((t2 - t1)/1000).toInt} us")
    t
  }
}
