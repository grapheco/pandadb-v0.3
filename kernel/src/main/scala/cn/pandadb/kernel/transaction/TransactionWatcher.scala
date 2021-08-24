package cn.pandadb.kernel.transaction

import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicInteger

import cn.pandadb.kernel.util.CommonUtils

/**
 * @program: pandadb-v0.3
 * @description: watch tx number, when tx number = 0, clean undo log and guard log
 * @author: LiamGao
 * @create: 2021-08-24 11:11
 */
class TransactionWatcher(logPath: String) extends Runnable{
  private val transactionCount = new AtomicInteger(0)
  var isCleaning = false

  def increase(): Unit ={
    transactionCount.incrementAndGet()
  }

  def decrease(): Unit ={
    transactionCount.decrementAndGet()
  }
  def get(): Int ={
    transactionCount.get()
  }

  override def run(): Unit = {
    if (transactionCount.get() == 0){
      isCleaning = true
      CommonUtils.cleanFileContent(s"$logPath/${DBNameMap.undoLogName}")
      CommonUtils.cleanFileContent(s"$logPath/${DBNameMap.guardLogName}")
      isCleaning = false
    }
  }
}
