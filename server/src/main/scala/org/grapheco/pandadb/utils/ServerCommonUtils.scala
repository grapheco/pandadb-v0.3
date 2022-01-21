package org.grapheco.pandadb.utils

import java.net.{DatagramSocket, NetworkInterface}
import org.grapheco.pandadb.kernel.udp.UDPServer
import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException
import org.grapheco.pandadb.server.common.configuration.Config

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-12-30 16:19
 */
object ServerCommonUtils {
  val r1 = "(explain\\s+)?match\\s*\\(.*\\s*\\{?.*\\}?\\s*\\)\\s*(where)?\\s*.*\\s*(set|remove|delete|merge)\\s*"
  val r2 = "(explain\\s+)?merge\\s*\\(.*\\s*\\{?.*\\}?\\s*\\)\\s*(where)?\\s*.*\\s*(set|remove|delete|merge)?\\s*"
  val r3 = "(explain\\s+)?create\\s*\\(.*\\{?.*\\}?\\s*\\)"
  val r4 = "create index on"
  val pattern = new Regex(s"${r1}|${r2}|${r3}|${r4}")
  var serverLocalIp: String = _
  var serverLocalPort: Int = _

  def getLocalIP(): Array[String] = {
    val localIps = ArrayBuffer[String]()
    val netInterfaces = NetworkInterface.getNetworkInterfaces()
    while (netInterfaces.hasMoreElements){
      val ni = netInterfaces.nextElement()
      val nii = ni.getInetAddresses()
      while (nii.hasMoreElements){
        val ip = nii.nextElement()
        localIps.append(ip.getHostAddress)
      }
    }
    localIps.toArray
  }

  def getClusterNodesIP(config: Config): (Boolean, Array[(String, Int)]) ={
    val addresses = config.getPandaNodes().split(",")
    val ipAndPort = addresses.map(f => {
      val array = f.split(":")
      (array.head, array.last.toInt)
    })

    val isLocal = addresses.forall(f => f.startsWith("127.0.0.1"))
    if (isLocal){
      (true, ipAndPort.sortBy(f => f._2))
    }
    else {
      if (ipAndPort.length != ipAndPort.map(f => f._1).toSet.size) throw new PandaDBException("check your config file of dbms.panda.nodes, all ip should be different")
      val localNetIp = getLocalIP()
      val localIp = ipAndPort.filter(p => {
        if (localNetIp.contains(p._1)) {
          serverLocalIp = p._1
          serverLocalPort = p._2
          true
        }
        else false
      })
      if (localIp.nonEmpty){
        val localPort = localIp.head._2
        val otherIps = ipAndPort.filterNot(p => localNetIp.contains(p._1))
        (false, Array((serverLocalIp, serverLocalPort)) ++ otherIps)
      }
      else throw new PandaDBException(s"you should start panda node at $serverLocalIp ")
    }
  }

  def isWriteStatement(statement: String): Boolean = {
    val cypher = statement.toLowerCase().replaceAll("\n", "").replaceAll("\r", "")
    pattern.findAllIn(cypher).nonEmpty
  }

  def isUDPPortUsing(port: Int): Boolean ={
    var flag = false
    try {
      val tmp = new DatagramSocket(port)
      tmp.close()
    }
    catch {
      case e: Exception => {flag = true}
    }
    flag
  }
}
