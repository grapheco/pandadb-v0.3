package cn.pandadb.driver.test

import cn.pandadb.driver.CypherErrorException
import org.junit.{After, Assert, Before, Test}
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase, Session}

import scala.collection.JavaConverters._

class DriverFunctionTest {

  var driver:Driver = _
  var session: Session = _

  @Before
  def init(): Unit ={
    driver = GraphDatabase.driver("panda://127.0.0.1:9989", AuthTokens.basic("pandadb", "pandadb"))
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
    Assert.assertEquals(Set("n", "n.name", "n.age"), res.keys().asScala.toSet)
  }

  @Test(expected = classOf[CypherErrorException])
  def errorCypherTest(): Unit ={
    val session = driver.session()
    session.run("match n return n")
  }
}
