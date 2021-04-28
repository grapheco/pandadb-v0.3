package cn.pandadb.test.cypher.cs

import org.junit.{After, Assert, Before, Test}
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Session}
import scala.collection.JavaConverters._
/**
 * @program: pandadb-v0.3
 * @description: driver test
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
  def testDataStructure(): Unit ={
    val res1 = session.run(
      """
        |create (n:person:people{
        |name:'alex',
        | money1:100,
        | money2:233.3,
        | flag:true,
        | money11:[11,22,33,44],
        | money22:[22.1, 33.2, 44.3],
        | flags:[true, true, false],
        | jobs:['teacher', 'singer', 'player'],
        | hybridArr:[1, 2.0, "3.0", true]
        | }) return n
        | """.stripMargin).next().get("n").asNode()
    Assert.assertEquals("alex", res1.get("name").asString())
    Assert.assertEquals(100, res1.get("money1").asInt())
    Assert.assertEquals(233.3, res1.get("money2").asDouble(), 1)
    Assert.assertEquals(true, res1.get("flag").asBoolean())
    Assert.assertEquals(List(11,22,33,44), res1.get("money11").asList().asScala)
    Assert.assertEquals(List(22.1, 33.2, 44.3), res1.get("money22").asList().asScala)
    Assert.assertEquals(List(true, true, false), res1.get("flags").asList().asScala)
    Assert.assertEquals(List("teacher", "singer", "player"), res1.get("jobs").asList().asScala)
    Assert.assertEquals(List(1, 2.0, "3.0", true), res1.get("hybridArr").asList().asScala)


    val res2 = session.run("match (n:people) return n").next().get("n").asNode()
    Assert.assertEquals("alex", res2.get("name").asString())
    Assert.assertEquals(100, res2.get("money1").asInt())
    Assert.assertEquals(233.3, res2.get("money2").asDouble(), 1)
    Assert.assertEquals(true, res2.get("flag").asBoolean())
    Assert.assertEquals(List(11,22,33,44), res2.get("money11").asList().asScala)
    Assert.assertEquals(List(22.1, 33.2, 44.3), res2.get("money22").asList().asScala)
    Assert.assertEquals(List(true, true, false), res2.get("flags").asList().asScala)
    Assert.assertEquals(List("teacher", "singer", "player"), res2.get("jobs").asList().asScala)
    Assert.assertEquals(List(1, 2.0, "3.0", true), res2.get("hybridArr").asList().asScala)  }

  @After
  def close(): Unit ={
    session.close()
  }
}
