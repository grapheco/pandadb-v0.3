package cn.pandadb.utils

import java.net.InetAddress

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

  def getOtherClusterNodesIP(config: Config): Array[(String, Int)] ={
    val localNetIp = getLocalIP()
    val port = config.getUDPPort()
    val tmp = config.getPandaNodes().split(",").filterNot(ip => {
      if (ip.startsWith("127.0.0.1")) ip == s"127.0.0.1:$port"
      else ip == s"$localNetIp:$port"
    })
    if (tmp.length > 0) {
      tmp.map(s => {
        val t = s.split(":")
        (t(0), t(1).toInt)
      })
    }
    else Array.empty
  }

  def isWriteStatement(statement: String): Boolean = {
    val cypher = statement.toLowerCase().replaceAll("\n", "").replaceAll("\r", "")
    pattern.findAllIn(cypher).nonEmpty
  }
}
