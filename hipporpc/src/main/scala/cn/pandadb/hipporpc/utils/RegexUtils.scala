package cn.pandadb.hipporpc.utils

import cn.pandadb.NotValidAddressException

import scala.util.matching.Regex

object RegexUtils {
  val uriRegex = "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)"
  val uriRegex2 = "(localhost)\\:(\\d+)"

  val pattern = new Regex(s"${uriRegex}|${uriRegex2}")
  def getIpAndPort(uri: String): (String, Int) ={
    try {
      val res = pattern.findFirstMatchIn(uri)
      val addr = res.get.group(0).split(":")
      (addr(0), addr(1).toInt)
    }
    catch {
      case e:Exception => throw new NotValidAddressException
    }
  }
}
