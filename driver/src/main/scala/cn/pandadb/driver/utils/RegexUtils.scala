package cn.pandadb.driver.utils

import cn.pandadb.driver.NotValidAddressException

import scala.util.matching.Regex

object RegexUtils {
  val uriRegex = new Regex("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)")

  def getIpAndPort(uri: String): (String, Int) ={
    val uriTemp = {
      if (uri.toLowerCase.contains("localhost")) uri.toLowerCase().replaceAll("localhost", "0.0.0.0")
      else uri.toLowerCase()
    }
    val res = uriRegex.findFirstMatchIn(uriTemp)
    if (res.isDefined){
      (res.get.group(1), res.get.group(2).toInt)
    }
    else{
      throw new NotValidAddressException
    }
  }
}
