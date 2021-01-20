import org.junit.Test
import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Values}

class TestDriver1x {
  @Test
  def test(): Unit ={
    val driver = GraphDatabase.driver("panda://localhost:9989", AuthTokens.basic("pandadb", "pandadb"))
    val session = driver.session()

//    val res1 = session.run("match (n) where n.name={nn} return n.name", Values.parameters("nn", "alex"))
    val res1 = session.run("match (n) return n, n.name, n.age")
//    println(res1.keys())
//    val res2 = session.run("match (n) return n")
//    println(res2.keys())
    while (res1.hasNext){
      val record = res1.next()
      println(record)
    }
    session.close()
    driver.close()
  }

  @Test
  def testOriginDriver(): Unit ={
    val driver = GraphDatabase.driver("bolt://localhost:8878", AuthTokens.basic("neo4j", "123456"))
    val session = driver.session()

//    val res = session.run("match (n:label0) where n.idStr=$NNN return n limit 1", Values.parameters("NNN", "caa"))
      val res = session.run("return 1")
//
    println(res.summary().server().address(), res.summary().server().version())
    println("===================")
    println(res.consume())
    while (res.hasNext){
      val record = res.next()
      println(record.keys(), record.values(), record.fields())
    }
    session.close()
    driver.close()
  }

  @Test
  def tempTest(): Unit ={

  }
}
