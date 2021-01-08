package cn.pandadb.driver.utils

import org.apache.logging.log4j.scala.Logging

object Profiler extends Logging {
  def timing[T](f: => T): T = {
    val t1 = System.currentTimeMillis()
    val t = f
    val t2 = System.currentTimeMillis()
    logger.debug(s"time cost: ${t2 - t1} ms")
    t
  }

  def timingByMicroSec[T](f: => T): T = {
    val t1 = System.nanoTime()
    val t = f
    val t2 = System.nanoTime()
    logger.debug(s"time cost: ${((t2 - t1)/1000).toInt} us")
    t
  }
}
