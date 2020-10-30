package cn.pandadb.kernel.util

import org.apache.logging.log4j.scala.Logging

object Profiler extends Logging {
  def timing[T](f: => T): T = {
    val t1 = System.currentTimeMillis()
    val t = f
    val t2 = System.currentTimeMillis()
    logger.debug(s"time cost: ${t2 - t1} ms")
    t
  }
}
