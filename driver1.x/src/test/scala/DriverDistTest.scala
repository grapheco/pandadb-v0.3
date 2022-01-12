import cn.pandadb.driver.{PandaDriver, PandaSession}
import org.junit.{After, Before, Test}
import org.neo4j.driver.v1.{AuthTokens, Driver, GraphDatabase}
import scala.collection.JavaConverters._
/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-11 15:38
 */
class DriverDistTest {
  var driver:PandaDriver  = _

  @Before
  def init(): Unit ={
//    driver = GraphDatabase.driver("panda://10.0.82.143:9989,10.0.82.144:9989,10.0.82.145:9989", AuthTokens.basic("pandadb", "pandadb")).asInstanceOf[PandaDriver]
    driver = GraphDatabase.driver("panda://localhost:9989,localhost:9990,localhost:9991", AuthTokens.basic("pandadb", "pandadb")).asInstanceOf[PandaDriver]
//    driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb")).asInstanceOf[PandaDriver]
//    driver = GraphDatabase.driver("panda://localhost:9990", AuthTokens.basic("pandadb", "pandadb")).asInstanceOf[PandaDriver]
//    driver = GraphDatabase.driver("panda://localhost:9991", AuthTokens.basic("pandadb", "pandadb")).asInstanceOf[PandaDriver]

  }
  @Test
  def cypher(): Unit ={
    val session = driver.session().asInstanceOf[PandaSession]
    val res = session.run("match (n) return n")
    while (res.hasNext){
      val data = res.next()
      val node = data.get("n").asNode()
      println(node.labels().asScala.toList, node.asMap().asScala.toList)
    }
  }

  @Test
  def addData(): Unit ={
    val session = driver.session().asInstanceOf[PandaSession]
    session.run(
      """
        |create (n1:person:worker{name:'a1',age:11})
        |create (n2:person:human{name:'a2',age:12, country:'China'})
        |create (n3:person:CNIC{name:'a3',age:13})
        |create (n4:person:worker{name:'a4',age:14})
        |create (n5:person:man{name:'a5',age:15})
        |""".stripMargin)
  }

  @Test
  def api(): Unit ={
    val session = driver.session().asInstanceOf[PandaSession]
//    val stat = session.getStatistics()
//    println(stat)
//    println(session.createIndex("person", Seq("age", "name")))
    session.dropIndex("person", "name")
    Thread.sleep(1000)
    session.dropIndex("person", "age")

  }
  @Test
  def api2(): Unit ={
    val session = driver.session().asInstanceOf[PandaSession]
    println(session.getIndexedMetaData)
    val stat = session.getStatistics()
    println(s"all nodes: ${stat.allNodes}")
    println(s"node by labels: ${stat.nodesCountByLabel.toList}")
    println(s"all relations: ${stat.allRelations}")
    println(s"relation by type: ${stat.relationsCountByType.toList} ")
    println(session.getStatistics().propertiesCountByIndex)
  }

  @After
  def close(): Unit ={
    driver.close()
  }
}
