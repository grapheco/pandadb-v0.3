package cn.pandadb.driver.test

import cn.pandadb.driver.{CypherErrorException, PandaDriver}
import org.junit.{After, Assert, Before, Test}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}

import scala.collection.JavaConverters._

class DriverFunctionTest {

  var driver:Driver = _
  var session: Session = _

  @Before
  def init(): Unit ={
    driver = GraphDatabase.driver("127.0.0.1:52000", AuthTokens.basic("panda", "db"))
    session = driver.session()
  }
  @After
  def close(): Unit ={
    session.close()
    driver.close()
  }

  @Test
  def cypherMetadataTest(): Unit ={
    val res = session.run("match (n) return n, n.name, n.age")
    Assert.assertEquals(Set("n", "n.name", "n.age"), res.next().keys().asScala.toSet)
  }

  @Test(expected = classOf[CypherErrorException])
  def errorCypherTest(): Unit ={
    val session = driver.session()
    val res = session.run("match n return n")
  }
}
