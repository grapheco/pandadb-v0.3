package org.grapheco.pandadb.kernel.udp

import java.net.{DatagramPacket, DatagramSocket}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.grapheco.pandadb.kernel.distribute.{DistributedGraphFacade, DistributedGraphService}
import com.typesafe.scalalogging.LazyLogging


/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-04 09:34
 */
class UDPServer(port: Int, db: DistributedGraphService) extends LazyLogging{
  val dataServer = new DatagramSocket(port)
  val data = new Array[Byte](1024)
  val dp = new DatagramPacket(data, data.length)
  var hasUdpMsg: Boolean = false

  val checkFlagService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val checkHasUdpMsg: Runnable = new Runnable {
    override def run(): Unit = {
      if (hasUdpMsg) {
        logger.debug("refresh meta ...")
        db.refreshMeta()
        hasUdpMsg = false
      }
    }
  }
  checkFlagService.scheduleAtFixedRate(checkHasUdpMsg, 0, 3, TimeUnit.SECONDS)

  def start(): Unit ={
    logger.debug(s"udp server started at port $port...")
    new Thread(){
      override def run(): Unit = {
        try {
          while (true){
            dataServer.receive(dp)
            val dataLength = dp.getLength
            val msg = new String(data, 0, dataLength)
            msg match {
              case UDPMsg.refreshMsg => hasUdpMsg = true
            }
          }
        }
        finally {
          dataServer.close()
        }
      }
    }.start()
  }

}
