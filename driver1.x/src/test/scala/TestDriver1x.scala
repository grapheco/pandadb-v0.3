import cn.pandadb.CypherErrorException
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}
import org.neo4j.driver.v1.types.Node
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Values}

import scala.collection.JavaConverters._
// run cn.pandadb.itest.ServerTest.testConfNoDBDescribe first.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestDriver1x {

  @Test
  def driverBasicTest1(): Unit = {
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()
    val res = session.run("match (n) return n")

    var count = 0
    while (res.hasNext){
      count += 1
      val data = res.next()
      println(data.get("n").asEntity().asMap().asScala.toList)
    }

//    Assert.assertEquals(0, count)
    session.close()
    driver.close()
  }

  @Test
  def transaction(): Unit ={
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()
    val tx = session.beginTransaction()
    tx.run("create (n:person{name:'google3'})")
    tx.success()
    session.close()
    driver.close()
  }

  @Test
  def driverBasicTest2(): Unit = {
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()
    session.run("create (n:person{name:'google1'})")
//    session.run("create (n:person{name:'google2'})")

//    Thread.sleep(5000)
//    val res = session.run("match (n:person) where n.name='google' return n")
//    Assert.assertEquals("google", res.next().get(0).get("name").asString())

    session.close()
    driver.close()
  }

  @Test(expected = classOf[CypherErrorException])
  def driverBasicTest3(): Unit ={
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()
    session.run("cre te (n:person{name:'google'})")
    session.close()
    driver.close()
  }

  @Test
  def driverOriginTest(): Unit ={
    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"))
    val session = driver.session()
    val res = session.run("match (n) return n")
    while (res.hasNext){
      println(res.next())
    }
  }
}
