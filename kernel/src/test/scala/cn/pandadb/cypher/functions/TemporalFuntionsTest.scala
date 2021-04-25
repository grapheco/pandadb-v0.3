package cn.pandadb.cypher.functions

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxDate, LynxDateTime, LynxLocalDateTime, LynxLocalTime, LynxTime}
import org.junit.{AfterClass, Assert, BeforeClass, Test}


object TemporalFuntionsTest {
  var db: GraphFacade = null
  val dbPath = "testdata/db1"

  @BeforeClass
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
  }

  @AfterClass
  def closeDB():Unit = {
    db.close()
  }
}

class TemporalFuntionsTest {
  val db = TemporalFuntionsTest.db

  @Test
  def testDate(): Unit = {
    db.cypher("CREATE (n:person{name:'date1', born:date('2015-02-01')})").show()
    db.cypher("CREATE (n:person{name:'date2', born:date('2015/02/02')})").show()

    val r1 = db.cypher("match (n:person{name:'date1'}) return n.born").records().next()
    Assert.assertEquals(LocalDate.parse("2015-02-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")),
      r1.get("n.born").get.asInstanceOf[LynxDate].value)
    val r2 = db.cypher("match (n:person{name:'date2'}) return n.born").records().next()
    Assert.assertEquals(LocalDate.parse("2015-02-02", DateTimeFormatter.ofPattern("yyyy-MM-dd")),
      r2.get("n.born").get.asInstanceOf[LynxDate].value)
  }

  @Test
  def testDateTime(): Unit = {
    db.cypher("CREATE (Keanu:person {name:'datetime1',born:datetime('1984-10-11T12:31:14.123456789Z')})").show()
    db.cypher("CREATE (Keanu:person {name:'datetime2',born:datetime('1984-10-11T12:31:14.123456789+01:00')})").show()

    val r1 = db.cypher("match (n:person{name:'datetime1'}) return n.born").records().next()
    Assert.assertEquals(ZonedDateTime.parse("1984-10-11T12:31:14.123456789Z"),
      r1.get("n.born").get.asInstanceOf[LynxDateTime].value)
    val r2 = db.cypher("match (n:person{name:'datetime2'}) return n.born").records().next()
    Assert.assertEquals(ZonedDateTime.parse("1984-10-11T12:31:14.123456789+01:00"),
      r2.get("n.born").get.asInstanceOf[LynxDateTime].value)
  }

  @Test
  def testLocalDateTime(): Unit = {
    db.cypher("CREATE (Keanu:person {name:'localdatetime1',born:localdatetime('2015-07-21T21:40:32.142')})").show()
    val r1 = db.cypher("match (n:person{name:'localdatetime1'}) return n.born").records().next()
    Assert.assertEquals(LocalDateTime.parse("2015-07-21T21:40:32.142"),
      r1.get("n.born").get.asInstanceOf[LynxLocalDateTime].value)
  }

  @Test
  def testLocalTime(): Unit = {
    db.cypher("CREATE (Keanu:person {name:'localtime1',born: localtime('21:40:32.142')})").show()
    val r1 = db.cypher("match (n:person{name:'localtime1'}) return n.born").records().next()
    Assert.assertEquals(LocalTime.parse("21:40:32.142"),
      r1.get("n.born").get.asInstanceOf[LynxLocalTime].value)
  }

  @Test
  def testTime(): Unit = {
    db.cypher("CREATE (Keanu:person {name:'time1',born: time('21:40:32.142+01:00')})").show()
    val r1 = db.cypher("match (n:person{name:'time1'}) return n.born").records().next()
    Assert.assertEquals(OffsetTime.parse("21:40:32.142+01:00"),
      r1.get("n.born").get.asInstanceOf[LynxTime].value)
  }

}
