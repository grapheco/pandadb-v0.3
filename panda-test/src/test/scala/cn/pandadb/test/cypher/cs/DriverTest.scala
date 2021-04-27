package cn.pandadb.test.cypher.cs

import org.junit.{After, Assert, Before, Test}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Session}

/**
 * @program: pandadb-v0.3
 * @description: ${description}
 * @author: LiamGao
 * @create: 2021-04-26
 */
class DriverTest {
  val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("", ""))
  var session: Session = _

  @Before
  def init(): Unit ={
    session = driver.session()
  }
  @Test
  def testCreate(): Unit ={
    session.run("create (n:person{name:'alex', money:[1,2,3,4,5]}) return n")
    println(session.run("match (n) return n").next().get("n").asMap())
  }
  @Test
  def testDelete(): Unit ={
    session.run("delete (n) return n")
    Assert.assertEquals(0, session.run("match (n) return n").stream().count())
  }
  @After
  def close(): Unit ={
    session.close()
  }
}
