package cn.pandadb.utils

import java.net.{DatagramSocket, InetAddress, Socket}

import cn.pandadb.kernel.udp.UDPServer
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import cn.pandadb.server.common.configuration.Config

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

  def getLocalIP(): String = {
    InetAddress.getLocalHost().getHostAddress
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
      val localIp = ipAndPort.filter(p => p._1 == localNetIp)
      if (localIp.nonEmpty){
        val localPort = localIp.head._2
        val otherIps = ipAndPort.filterNot(p => p._1 == localNetIp)
        (false, Array((localNetIp, localPort)) ++ otherIps)
      }
      else throw new PandaDBException(s"you should start panda node at $localNetIp ")
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
