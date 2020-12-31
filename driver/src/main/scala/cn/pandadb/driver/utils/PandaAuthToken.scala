package cn.pandadb.driver.utils

object PandaAuthToken {

  def basic(username: String, password: String): PandaAuthToken ={
    new PandaAuthToken(username, password)
  }
}

class PandaAuthToken(usr: String, pwd: String) extends Serializable {
  val username: String = usr
  val password: String = pwd
}
