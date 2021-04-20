package cn.pandadb.cypher.functions

import java.io.File

import cn.pandadb.kernel.GraphDatabaseBuilder
import cn.pandadb.kernel.kv.GraphFacade
import org.apache.commons.io.FileUtils
import org.junit.{AfterClass, Assert, BeforeClass, Test}

object StringFunctionsTest {
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

class StringFunctionsTest {
  val db = StringFunctionsTest.db

  @Test
  def leftTest(): Unit = {
    // left() returns a string containing the specified number of leftmost characters of the original string.
    //Syntax: left(original, length)
    val cypher = """RETURN left('hello', 3)""".stripMargin
    val res = db.cypher(cypher).records()
    Assert.assertEquals("hel", res.next().get("left('hello', 3)").get)
  }

  @Test
  def lTrimTest(): Unit = {
    // lTrim() returns the original string with leading whitespace removed.
    //Syntax: lTrim(original)
    val cypher = """RETURN lTrim('   hello')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.size)
    Assert.assertEquals("hello", res(0).get("lTrim('   hello')").get)
  }

  @Test
  def replaceTest(): Unit = {
    // replace() returns a string in which all occurrences of a specified string in the original string
    // have been replaced by another (specified) string.
    //Syntax: replace(original, search, replace)
    val cypher = """RETURN replace("hello", "l", "w")""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("hewwo", res(0).get("replace(\"hello\", \"l\", \"w\")").get)
  }

  @Test
  def reverseTest(): Unit = {
    // reverse() returns a string in which the order of all characters in the original string have been reversed.
    //Syntax: reverse(original)
    val cypher = """RETURN reverse('anagram')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("margana", res(0).get("reverse('anagram')").get)
  }

  @Test
  def rightTest(): Unit = {
    // right() returns a string containing the specified number of rightmost characters of the original string.
    //Syntax: right(original, length)
    val cypher = """RETURN right('hello', 3)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("llo", res(0).get("right('hello', 3)").get)
  }

  @Test
  def rTrimTest(): Unit = {
    // rTrim() returns the original string with trailing whitespace removed.
    //Syntax: rTrim(original)
    val cypher = """RETURN rTrim('hello   ')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("hello", res(0).get("rTrim('hello   ')").get)
  }

  @Test
  def splitTest(): Unit = {
    // split() returns a list of strings resulting from the splitting of the original string around matches of the given delimiter.
    //Syntax: split(original, splitDelimiter)
    val cypher = """RETURN split('one,two', ',')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def substringTest(): Unit = {
    // substring() returns a substring of the original string, beginning with a 0-based index start and length.
    //Syntax: substring(original, start [, length])
    val cypher = """RETURN substring('hello', 1, 3), substring('hello', 2)""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
  }

  @Test
  def toLowerTest(): Unit = {
    // toLower() returns the original string in lowercase.
    //Syntax: toLower(original)
    val cypher = """RETURN toLower('HELLO')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("hello", res(0).get("toLower('HELLO')").get)
  }

  @Test
  def toStringTest(): Unit = {
    // toString() converts an integer, float or boolean value to a string.
    //Syntax: toString(expression)
    val cypher = """RETURN toString(11.5),
                   |toString('already a string'),
                   |toString(true),
                   |toString(date({year:1984, month:10, day:11})) AS dateString,
                   |toString(datetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, millisecond: 341, timezone: 'Europe/Stockholm'})) AS datetimeString,
                   |toString(duration({minutes: 12, seconds: -60})) AS durationString""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("11.5", res(0).get("toString(11.5)").get)
    Assert.assertEquals("already a string", res(0).get("toString('already a string')").get)
    Assert.assertEquals("true", res(0).get("toString(true)").get)
    Assert.assertEquals("1984-10-11", res(0).get("dateString").get)
    Assert.assertEquals("1984-10-11T12:31:14.341+01:00[Europe/Stockholm]", res(0).get("datetimeString").get)
    Assert.assertEquals("PT11M", res(0).get("durationString").get)
  }

  @Test
  def toUpperTest(): Unit = {
    // toUpper() returns the original string in uppercase.
    //Syntax: toUpper(original)
    val cypher = """RETURN toUpper('hello')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("HELLO", res(0).get("toUpper('hello')").get)
  }

  @Test
  def trimTest(): Unit = {
    // trim() returns the original string with leading and trailing whitespace removed.
    //Syntax: trim(original)
    val cypher = """RETURN trim('   hello   ')""".stripMargin
    val res = db.cypher(cypher).records().toArray
    Assert.assertEquals(1, res.length)
    Assert.assertEquals("hello", res(0).get("trim(' hello ')").get)
  }

}
