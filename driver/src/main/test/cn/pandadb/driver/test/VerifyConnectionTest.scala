package cn.pandadb.driver.test

import cn.pandadb.driver.{NotValidAddressException, PandaAuthToken, UsernameOrPasswordErrorException}
import net.neoremind.kraps.RpcException
import org.junit.{Before, Test}
import org.neo4j.driver.GraphDatabase

class VerifyConnectionTest {

  @Test
  def verifyIPTest(): Unit ={
    GraphDatabase.driver("127.0.0.1:8878", PandaAuthToken.basic("panda", "db")).close
  }

  @Test
  def verifyLocalhostTest(): Unit ={
    GraphDatabase.driver("localhost:8878", PandaAuthToken.basic("panda", "db")).close
  }

  @Test
  def verifyDefaultAddressTest(): Unit ={
    GraphDatabase.driver("0.0.0.0:8878", PandaAuthToken.basic("panda", "db")).close
  }

  @Test(expected = classOf[NotValidAddressException])
  def errorIpTest(): Unit ={
      GraphDatabase.driver(":8878", PandaAuthToken.basic("panda", "db")).close
  }

  @Test(expected = classOf[RpcException])
  def errorIpTest2(): Unit ={
    GraphDatabase.driver("1.1.1.1:8878", PandaAuthToken.basic("panda", "db")).close
  }

  @Test(expected = classOf[UsernameOrPasswordErrorException])
  def errorAccountTest(): Unit ={
    GraphDatabase.driver("0.0.0.0:8878", PandaAuthToken.basic("123", "db")).close
  }

}
