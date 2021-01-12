package cn.pandadb.driver.startup

import org.neo4j.driver.{AuthTokens, GraphDatabase}

object RunDriver {
  def main(args: Array[String]): Unit = {
    val ip:String = args(0)
    val port:String = args(1)
    val cypher:String = args(2)
    val account = args(3)
    val password = args(4)

    val driver = GraphDatabase.driver(ip + ":" + port, AuthTokens.basic(account, password))
    val session = driver.session()
    val res = session.run(cypher)
    while (res.hasNext){
      println(res.next())
    }
    session.close()
    driver.close()
  }
}
