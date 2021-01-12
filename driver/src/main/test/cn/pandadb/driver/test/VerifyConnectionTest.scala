package cn.pandadb.driver.test

import cn.pandadb.driver.{NotValidAddressException, UsernameOrPasswordErrorException}
import net.neoremind.kraps.RpcException
import org.junit.{Before, Test}
import org.neo4j.driver.{AuthTokens, GraphDatabase}

class VerifyConnectionTest {
  val port = "8878"
  @Test
  def verifyIPTest(): Unit ={
    GraphDatabase.driver(s"127.0.0.1:$port", AuthTokens.basic("panda", "db")).close
  }

  @Test
  def verifyLocalhostTest(): Unit ={
    GraphDatabase.driver(s"localhost:$port", AuthTokens.basic("panda", "db")).close
  }

  @Test
  def verifyDefaultAddressTest(): Unit ={
    GraphDatabase.driver(s"0.0.0.0:$port", AuthTokens.basic("panda", "db")).close
  }

  @Test(expected = classOf[NotValidAddressException])
  def errorIpTest(): Unit ={
      GraphDatabase.driver(s":$port", AuthTokens.basic("panda", "db")).close
  }

  @Test(expected = classOf[RpcException])
  def errorIpTest2(): Unit ={
    GraphDatabase.driver(s"1.1.1.1:$port", AuthTokens.basic("panda", "db")).close
  }
  @Test(expected = classOf[RpcException])
  def errorPortTest(): Unit ={
    GraphDatabase.driver(s"0.0.0.0:123123", AuthTokens.basic("panda", "db")).close
  }

  @Test(expected = classOf[UsernameOrPasswordErrorException])
  def errorAccountTest(): Unit ={
    GraphDatabase.driver(s"0.0.0.0:$port", AuthTokens.basic("123", "db")).close
  }
}
