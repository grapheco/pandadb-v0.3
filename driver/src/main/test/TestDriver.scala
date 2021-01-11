import cn.pandadb.driver.{PandaAuthToken, PandaDriver}
import cn.pandadb.driver.utils.Types
import org.junit.Test
import org.neo4j.driver.{AuthToken, AuthTokens, GraphDatabase, Record, Values}
import org.neo4j.driver.internal.value.RelationshipValue
import org.neo4j.driver.types.Relationship

class TestDriver {
  @Test
  def test(): Unit ={
    val driver = GraphDatabase.driver("127.0.0.1:52000", PandaAuthToken.basic("panda", "db"))
    val session = driver.session()

//    val res1 = session.run("match (n) where n.name={nn} return n.name", Values.parameters("nn", "alex"))
    val res2 = session.run("match (n) return n, n.name, n.age")
    println(res2.keys())
    while (res2.hasNext){
      val record = res2.next()
      println(record)
    }

//    println(res1.next(), "~~~")
    session.close()
    driver.close()
  }

  @Test
  def testOriginDriver(): Unit ={
//    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123123"))
//    val session = driver.session()
//
//    val res = session.run("match (n:label0) where n.idStr=$NNN return n limit 1", Values.parameters("NNN", "caa"))
//
//    println(res.keys())
//    println(res.list())
//    println("++++++++++++++++++")
//    while (res.hasNext){
//      val record = res.next()
//      println(record)
//      println("===================")
//    }
//    session.close()
//    driver.close()
  }

  @Test
  def tempTest(): Unit ={

  }
}
