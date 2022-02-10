package org.grapheco.pandadb.kernel.udp

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.grapheco.pandadb.kernel.distribute.{DistributedGraphFacade, DistributedGraphService}
import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-12 13:54
 */
class UDPClientManager(clients: Array[UDPClient]) {
  private var db: DistributedGraphService = _
  private var hasOperation: Boolean = false
  private var currentOperationNum: Int = 0
  private val countOperation: AtomicInteger = new AtomicInteger(0)

  val checkFlagService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  val checkIsOperationFinished: Runnable = new Runnable {
    override def run(): Unit = {
      if (hasOperation && currentOperationNum == countOperation.get()) {
        hasOperation = false
        currentOperationNum = 0
        countOperation.set(0)
        sendMsg()
      }
    }
  }
  checkFlagService.scheduleAtFixedRate(checkIsOperationFinished, 0, 3, TimeUnit.SECONDS)


  val scheduleAddService : ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val addNum: Runnable = new Runnable {
    override def run(): Unit = {
      currentOperationNum = countOperation.get()
    }
  }
  scheduleAddService.scheduleAtFixedRate(addNum, 0, 1, TimeUnit.SECONDS)


  def setDB(d: DistributedGraphService): Unit ={
    db = d
  }
  // receive all kind of operation
  def sendRefreshMsg(): Unit ={
      hasOperation = true
      countOperation.incrementAndGet()
  }

  def sendMsg(): Unit ={
    db.getStatistics.flush()
    clients.foreach(c => c.sendRefreshMsg())
  }

  def close(): Unit ={
    clients.foreach(c => c.close())
  }
}
