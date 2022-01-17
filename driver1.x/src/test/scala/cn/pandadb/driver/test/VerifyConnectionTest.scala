package cn.pandadb.driver.test

import cn.pandadb.NotValidAddressException
import cn.pandadb.driver.{NotValidAddressException, UsernameOrPasswordErrorException}
import net.neoremind.kraps.RpcException
import org.junit.runners.MethodSorters
import org.junit.{FixMethodOrder, Test}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class VerifyConnectionTest {
  val port = "9989"
  val username = "pandadb"
  val password = "pandadb"

  @Test
  def _1changePasswordTest(): Unit ={
    GraphDatabase.driver(s"panda://127.0.0.1:$port", AuthTokens.basic(username, password)).close()
  }

  @Test
  def verifyIPTest(): Unit ={
    GraphDatabase.driver(s"panda://127.0.0.1:$port", AuthTokens.basic(username, password)).close
  }

  @Test
  def verifyLocalhostTest(): Unit ={
    GraphDatabase.driver(s"panda://localhost:$port", AuthTokens.basic(username, password)).close
  }

  @Test
  def verifyDefaultAddressTest(): Unit ={
    GraphDatabase.driver(s"panda://0.0.0.0:$port", AuthTokens.basic(username, password)).close
  }

  @Test(expected = classOf[NotValidAddressException])
  def errorIpTest(): Unit ={
      GraphDatabase.driver(s"panda://:$port", AuthTokens.basic(username, password)).close
  }

  @Test(expected = classOf[RpcException])
  def errorIpTest2(): Unit ={
    GraphDatabase.driver(s"panda://1.1.1.1:$port", AuthTokens.basic(username, password)).close
  }
  @Test(expected = classOf[RpcException])
  def errorPortTest(): Unit ={
    GraphDatabase.driver(s"panda://0.0.0.0:123123", AuthTokens.basic(username, password)).close
  }

  @Test(expected = classOf[UsernameOrPasswordErrorException])
  def errorAccountTest(): Unit ={
    GraphDatabase.driver(s"panda://0.0.0.0:$port", AuthTokens.basic(username, "aaa")).close
  }
}
