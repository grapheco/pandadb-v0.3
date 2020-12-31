import cn.pandadb.driver.PandaDriver
import cn.pandadb.driver.utils.{PandaAuthToken, Types}
import org.junit.Test
import org.neo4j.driver.{AuthToken, AuthTokens, GraphDatabase, Record}
import org.neo4j.driver.internal.value.RelationshipValue
import org.neo4j.driver.types.Relationship

class TestDriver {
  @Test
  def test(): Unit ={
    val driver = GraphDatabase.driver("localhost:52000", PandaAuthToken.basic("panda", "db"))
    val session = driver.session()
//    val res = session.run("match (n) return n")
//    val res = session.run("match (n) where n.age=10 return n")
//    val res = session.run("match (n:person) where n.age=10 return n")
    val res = session.run("match (n:perwwson) where n.age=10 return n")

    //    val res = session.run("create (n:person{age:10, name:'bob'})")

    while (res.hasNext){
      val record = res.next()
      println(record)
    }
    session.close()
    driver.close()
  }

  @Test
  def testOriginDriver(): Unit ={
//    val driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123123"))
//    val session = driver.session()
//    val res = session.run("match (n) where n.age=1000 return n")
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
