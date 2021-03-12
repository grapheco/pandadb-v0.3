package cn.pandadb.cypher.functions

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}

object AggregatingFunctionsTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
    initDataByAPI(db)
  }

  def initDataByAPI(db: GraphFacade): Unit = {
    val n1 = db.addNode(Map("name"->"A", "age"->13), "Person")
    val n2 = db.addNode(Map("name"->"B", "eyes"->"blue", "age"->33), "Person")
    val n3 = db.addNode(Map("name"->"C", "eyes"->"blue", "age"->44), "Person")
    val n4 = db.addNode(Map("name"->"D", "eyes"->"brown"), "Person")
    val n5 = db.addNode(Map("name"->"D"), "Person")


    val r1 = db.addRelation("KNOWS", n1, n2, Map())
    val r2 = db.addRelation("KNOWS", n1, n3, Map())
    val r3 = db.addRelation("KNOWS", n1, n4, Map())
    val r4 = db.addRelation("KNOWS", n2, n5, Map())
    val r5 = db.addRelation("KNOWS", n3, n5, Map())
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}


class AggregatingFunctionsTest {
  val db = AggregatingFunctionsTest.db

  @Test
  def avgTest1(): Unit = {
    // avg() returns the average of a set of numeric values.
    //Syntax: avg(expression)
    val cypher = """MATCH (n:Person) RETURN avg(n.age)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(30.0, res.next().get("avg(n.age)").get)
  }

  @Test
  def avgTest2(): Unit = {
    // avg() returns the average of a set of Durations.
    //Syntax: avg(expression)
    val cypher = """UNWIND [duration('P2DT3H'), duration('PT1H45S')] AS dur RETURN avg(dur)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals(1, res.size)
  }

  @Test
  def collectTest(): Unit = {
    // collect() returns a list containing the values returned by an expression.
    // Using this function aggregates data by amalgamating multiple records or values into a single list.
    //Syntax: collect(expression)
    val cypher = """MATCH (n:Person) RETURN collect(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def countTest1(): Unit = {
    // Using count(*) to return the number of nodes
    val cypher = """MATCH (n {name: 'A'})-->(x) RETURN labels(n), n.age, count(*)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3, res(0).get("count(*)").get)
  }

  @Test
  def countTest2(): Unit = {
    // Using count(*) to group and count relationship types
    val cypher = """MATCH (n {name: 'A'})-[r]->() RETURN type(r), count(*)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3, res(0).get("count(*)").get)
  }

  @Test
  def countTest3(): Unit = {
    //  Using count(expression) to return the number of values
    val cypher = """MATCH (n {name: 'A'})-->(x) RETURN count(x)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3, res(0).get("count(x)").get)
  }

  @Test
  def countTest4(): Unit = {
    //  Counting non-null values
    val cypher = """MATCH (n:Person) RETURN count(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(3, res(0).get("count(n.age)").get)
  }

  @Test
  def countTest5(): Unit = {
    //  Counting with and without duplicates
    val cypher = """MATCH (me:Person)-->(friend:Person)-->(friend_of_friend:Person)
                   |WHERE me.name = 'A'
                   |RETURN count(DISTINCT friend_of_friend), count(friend_of_friend)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(2, res(0).get("count(friend_of_friend)").get)
  }

  @Test
  def maxTest1(): Unit = {
    // max() returns the maximum value in a set of values.
    // Syntax: max(expression)
    val cypher = """UNWIND [1, 'a', null, 0.2, 'b', '1', '99'] AS val RETURN max(val)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(1, res(0).get("max(val)").get)
  }

  @Test
  def maxTest2(): Unit = {
    // max() returns the maximum value in a set of values.
    // Syntax: max(expression)
    val cypher = """UNWIND [[1, 'a', 89], [1, 2]] AS val RETURN max(val)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(List[Int](1,2), res(0).get("max(val)").get)
  }

  @Test
  def maxTest3(): Unit = {
    // max() returns the maximum value in a set of values.
    // Syntax: max(expression)
    val cypher = """MATCH (n:Person) RETURN max(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(44, res(0).get("max(n.age)").get)
  }

  @Test
  def minTest1(): Unit = {
    // min() returns the minimum value in a set of values.
    //Syntax: min(expression)
    val cypher = """UNWIND [1, 'a', null, 0.2, 'b', '1', '99'] AS val RETURN min(val)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("1", res(0).get("min(val)").get)
  }

  @Test
  def minTest2(): Unit = {
    // min() returns the minimum value in a set of values.
    //Syntax: min(expression)
    val cypher = """UNWIND ['d', [1, 2], ['a', 'c', 23]] AS val RETURN min(val)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def minTest3(): Unit = {
    // min() returns the minimum value in a set of values.
    //Syntax: min(expression)
    val cypher = """MATCH (n:Person) RETURN min(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(13, res(0).get("min(n.age)").get)
  }

  @Test
  def percentileContTest(): Unit = {
    // percentileCont() returns the percentile of the given value over a group, with a percentile from 0.0 to 1.0.
    //Syntax: percentileCont(expression, percentile)
    val cypher = """MATCH (n:Person) RETURN percentileCont(n.age, 0.4)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(29.0, res(0).get("percentileCont(n.age, 0.4)").get)
  }

  @Test
  def percentileDiscTest(): Unit = {
    // percentileDisc() returns the percentile of the given value over a group, with a percentile from 0.0 to 1.0.
    //Syntax: percentileDisc(expression, percentile)
    val cypher = """MATCH (n:Person) RETURN percentileDisc(n.age, 0.5)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(33, res(0).get("percentileDisc(n.age, 0.5)").get)
  }

  @Test
  def stDevTest(): Unit = {
    val cypher = """MATCH (n) WHERE n.name IN ['A', 'B', 'C'] RETURN stDev(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def stDevPTest(): Unit = {
    val cypher = """MATCH (n) WHERE n.name IN ['A', 'B', 'C'] RETURN stDevP(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def sumTest1(): Unit = {
    // sum() returns the sum of a set of numeric values.
    //Syntax: sum(expression)
    val cypher = """MATCH (n:Person) RETURN sum(n.age)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals(33, res(0).get("sum(n.age)").get)
  }

  @Test
  def sumTest(): Unit = {
    // sum() returns the sum of a set of Durations.
    //Syntax: sum(expression)
    val cypher = """UNWIND [duration('P2DT3H'), duration('PT1H45S')] AS dur RETURN sum(dur)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

}
