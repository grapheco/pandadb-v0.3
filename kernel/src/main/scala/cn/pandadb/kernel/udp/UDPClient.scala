package cn.pandadb.kernel.udp

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.Charset

import com.typesafe.scalalogging.LazyLogging

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-04 09:34
 */
class UDPClient(serverIp: String, serverPort: Int) extends LazyLogging{
  private val inet = InetAddress.getByName(serverIp)
  private val ds = new DatagramSocket()

  def sendRefreshMsg(): Unit ={
    logger.info(s"send refresh udp to $serverIp:$serverPort")
    val data = UDPMsg.refreshMsg.getBytes(Charset.forName("utf-8"))
    val dp = new DatagramPacket(data, data.length, inet, serverPort)
    ds.send(dp)
  }
  def close(): Unit ={
    ds.close()
  }
}
