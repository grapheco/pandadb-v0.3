package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object WithTest{
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Anders"))
    val n2 = db.addNode(Map("name"->"Ceasar"))
    val n3 = db.addNode(Map("name"->"Bossman"))
    val n4 = db.addNode(Map("name"->"George"))
    val n5 = db.addNode(Map("name"->"David"))

    val r1 = db.addRelation("BLOCKS", n1, n2, Map())
    val r2 = db.addRelation("BLOCKS", n3, n5, Map())
    val r3 = db.addRelation("KNOWS", n1, n3, Map())
    val r4 = db.addRelation("KNOWS", n2, n4, Map())
    val r5 = db.addRelation("KNOWS", n3, n4, Map())
    val r6 = db.addRelation("KNOWS", n5, n1, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}


class WithTest {
  val db = WithTest.db

  @Test
  def test1(): Unit = {
    val cypher = """MATCH (david {name: 'David'})--(otherPerson)-->()
                   |WITH otherPerson, count(*) AS foaf
                   |WHERE foaf > 1
                   |RETURN otherPerson.name""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test2(): Unit = {
    val cypher = """MATCH (n)
                   |WITH n
                   |ORDER BY n.name DESC
                   |LIMIT 3
                   |RETURN collect(n.name)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.length)
  }

  @Test
  def test3(): Unit = {
    val cypher = """MATCH (n {name: 'Anders'})--(m)
                   |WITH m
                   |ORDER BY m.name DESC
                   |LIMIT 1
                   |MATCH (m)--(o)
                   |RETURN o.name""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(2, res.length)
  }

}
