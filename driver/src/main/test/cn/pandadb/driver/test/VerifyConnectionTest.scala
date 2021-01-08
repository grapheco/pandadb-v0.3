package cn.pandadb.driver.test

import cn.pandadb.driver.{NotValidAddressException, PandaAuthToken, UsernameOrPasswordErrorException}
import net.neoremind.kraps.RpcException
import org.junit.{Before, Test}
import org.neo4j.driver.GraphDatabase

class VerifyConnectionTest {
  val port = "8878"
  @Test
  def verifyIPTest(): Unit ={
    GraphDatabase.driver(s"127.0.0.1:$port", PandaAuthToken.basic("panda", "db")).close
  }

  @Test
  def verifyLocalhostTest(): Unit ={
    GraphDatabase.driver(s"localhost:$port", PandaAuthToken.basic("panda", "db")).close
  }

  @Test
  def verifyDefaultAddressTest(): Unit ={
    GraphDatabase.driver(s"0.0.0.0:$port", PandaAuthToken.basic("panda", "db")).close
  }

  @Test(expected = classOf[NotValidAddressException])
  def errorIpTest(): Unit ={
      GraphDatabase.driver(s":$port", PandaAuthToken.basic("panda", "db")).close
  }

  @Test(expected = classOf[RpcException])
  def errorIpTest2(): Unit ={
    GraphDatabase.driver(s"1.1.1.1:$port", PandaAuthToken.basic("panda", "db")).close
  }
  @Test(expected = classOf[RpcException])
  def errorPortTest(): Unit ={
    GraphDatabase.driver(s"0.0.0.0:123123", PandaAuthToken.basic("panda", "db")).close
  }

  @Test(expected = classOf[UsernameOrPasswordErrorException])
  def errorAccountTest(): Unit ={
    GraphDatabase.driver(s"0.0.0.0:$port", PandaAuthToken.basic("123", "db")).close
  }
}
