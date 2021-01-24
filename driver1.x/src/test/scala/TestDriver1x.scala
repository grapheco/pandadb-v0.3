import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Values}

// run cn.pandadb.itest.ServerEntryPointTest first.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestDriver1x {

  @Test
  def driverBasicTest1(): Unit = {
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()
    val res = session.run("match (n) return n")
    Assert.assertEquals(0,  res.stream().count())
    session.close()
    driver.close()
  }

  @Test
  def driverBasicTest2(): Unit = {
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()
    session.run("create (n:person{name:'google'})")
    val res = session.run("match (n:person) where n.name='google' return n")

    Assert.assertEquals("google", res.next().get(0).get("name").asString())

    session.close()
    driver.close()
  }
}
