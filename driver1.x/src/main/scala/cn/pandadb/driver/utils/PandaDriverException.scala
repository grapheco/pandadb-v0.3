package cn.pandadb.driver.utils

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-12-30 10:59
 */
class PandaDriverException(msg: String) extends Exception{
  override def getMessage: String = {
    msg
  }
}
