package cn.pandadb.cypher.functions

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}

object ListFunctionsTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"Alice", "eyes"->"brown", "age"->38), "Person", "Developer")
    val n2 = db.addNode(Map("name"->"Charlie", "eyes"->"green", "age"->53))
    val n3 = db.addNode(Map("name"->"Bob", "eyes"->"blue", "age"->25))
    val n4 = db.addNode(Map("name"->"Daniel", "eyes"->"brown", "age"->54))
    val n5 = db.addNode(Map("name"->"Eskil", "eyes"->"blue", "age"->41, "array"->Array[String]("one","two","three")))

    val r1 = db.addRelation("KNOWS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n1, n3, Map())
    val r3 = db.addRelation("KNOWS", n2, n4, Map())
    val r4 = db.addRelation("KNOWS", n3, n4, Map())
    val r5 = db.addRelation("MARRIED", n3, n5, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class ListFunctionsTest {
  val db = ListFunctionsTest.db

  @Test
  def keysTest(): Unit = {
    // keys returns a list containing the string representations for all the property names of a node, relationship, or map.
    //Syntax: keys(expression)
    val cypher = """MATCH (a) WHERE a.name = 'Alice'
                   |RETURN keys(a)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    val arr = Array("name", "eyes", "age")
    res(0).get("keys(a)").get.asInstanceOf[Iterable[String]].foreach(k=> Assert.assertTrue(arr.contains(k.toString)))
  }

  @Test
  def labelsTest(): Unit = {
    // labels returns a list containing the string representations for all the labels of a node.
    //Syntax: labels(node)
    val cypher = """MATCH (a) WHERE a.name = 'Alice'
                   |RETURN labels(a)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    val arr = Array("Person", "Developer")
    res(0).get("labels(a)").get.asInstanceOf[Iterable[String]].foreach(k=> Assert.assertTrue(arr.contains(k.toString)))
  }

  @Test
  def nodesTest(): Unit = {
    // nodes() returns a list containing all the nodes in a path.
    //Syntax: nodes(path)
    val cypher = """MATCH p = (a)-->(b)-->(c)
                   |WHERE a.name = 'Alice' AND c.name = 'Eskil'
                   |RETURN nodes(p)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def rangeTest(): Unit = {
    // range() returns a list comprising all integer values within a range bounded by a start value start
    // and end value end, where the difference step between any two consecutive values is constant;
    // Syntax: range(start, end [, step])
    val cypher = """RETURN range(0, 10), range(2, 18, 3), range(0, 5, -1)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def reduceTest(): Unit = {
    // reduce() returns the value resulting from the application of an expression
    // on each successive element in a list in conjunction with the result of the computation thus far.
    // Syntax: reduce(accumulator = initial, variable IN list | expression)
    val cypher = """MATCH p = (a)-->(b)-->(c)
                   |WHERE a.name = 'Alice' AND b.name = 'Bob' AND c.name = 'Daniel'
                   |RETURN reduce(totalAge = 0, n IN nodes(p) | totalAge + n.age) AS reduction""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(117, res(0).get("reduction").get)
  }

  @Test
  def relationshipsTest(): Unit = {
    // relationships() returns a list containing all the relationships in a path.
    //Syntax: relationships(path)
    val cypher = """MATCH p = (a)-->(b)-->(c)
                   |WHERE a.name = 'Alice' AND c.name = 'Eskil'
                   |RETURN relationships(p)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def reverseTest(): Unit = {
    // reverse() returns a list in which the order of all elements in the original list have been reversed.
    //Syntax: reverse(original)
    val cypher = """WITH [4923,'abc',521, null, 487] AS ids
                   |RETURN reverse(ids)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def tailTest(): Unit = {
    // tail() returns a list lresult containing all the elements, excluding the first one, from a list list.
    //Syntax: tail(list)
    val cypher = """MATCH (a) WHERE a.name = 'Eskil'
                   |RETURN a.array, tail(a.array)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

}
