import cn.pandadb.driver.utils.Types
import org.junit.Test
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.neo4j.driver.internal.value.RelationshipValue
import org.neo4j.driver.types.Relationship

class TestDriver {
  @Test
  def test(): Unit ={
    val driver = GraphDatabase.driver("bolt://127.0.0.1:8878", AuthTokens.basic("panda", "db"))
    val session = driver.session()

//    val res1 = session.run("match (n) where n.name={nn} return n.name", Values.parameters("nn", "alex"))
    val res1 = session.run("return 1")
    println(res1.keys())
//    val res2 = session.run("match (n) return n, n.age")
//    println(res2.keys())

    session.close()
    driver.close()
  }

  @Test
  def testOriginDriver(): Unit ={
    val driver = GraphDatabase.driver("bolt://localhost:8887", AuthTokens.basic("neo4j", "123"))
    val session = driver.session()

//    val res = session.run("match (n:label0) where n.idStr=$NNN return n limit 1", Values.parameters("NNN", "caa"))
      val res = session.run("match (n) return n")
//
    println(res.keys())
    println("===================")
    while (res.hasNext){
      val record = res.next()
      println(record.keys(), record.values(), record.fields(), record.asMap())
    }
    session.close()
    driver.close()
  }

  @Test
  def tempTest(): Unit ={

  }
}
