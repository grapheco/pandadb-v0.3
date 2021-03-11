package cn.pandadb.cypher.clauses

import java.io.File

import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService}
import org.apache.commons.io.FileUtils
import org.junit.{After, Before, Test}

class LoadCsvTest {

  var db: GraphService = null
  val csvDir = "loadcsv"
  val dbPath = "testdata/db1"

  @Before
  def initDB() = {
    FileUtils.deleteDirectory(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath)
  }

  @After
  def closeDB():Unit = {
    db.close()
  }

  @Test
  def loadCSVTests(): Unit = {
    val loadCsv =
      s"""LOAD CSV FROM '${csvDir}/artists.csv' AS line CREATE (:Artist {name: line[1],
                     year: toInteger(line[2])})"""

    val loadCsvWithHeaders =
      s"""LOAD CSV WITH HEADERS FROM '${csvDir}/artists-with-headers.csv' AS line CREATE (:Artist {name:
                     line.Name, year: toInteger(line.Year)})"""

    val  loadCsvWithCustomDelimiter =
      s"""LOAD CSV FROM '${csvDir}/artists-fieldterminator.csv' AS line FIELDTERMINATOR ';' CREATE (:Artist
         {name: line[1], year: toInteger(line[2])})"""

    val loadCsvUsingPeriodicCommit =
      s"""USING PERIODIC COMMIT LOAD CSV FROM '${csvDir}/artists.csv' AS line CREATE (:Artist {name: line[1],
           year: toInteger(line[2])})"""

    val loadCsvUsingPeriodicCommitSettingRate =
      s"""USING PERIODIC COMMIT 500 LOAD CSV FROM '${csvDir}/artists.csv' AS line CREATE (:Artist {name:
           line[1], year: toInteger(line[2])})"""

    val loadCsvContainingEscapedCharacters =
      s"""LOAD CSV FROM '${csvDir}/artists-with-escaped-char.csv' AS line CREATE (a:Artist {name: line[1],
         year: toInteger(line[2])}) RETURN a.name AS name, a.year as year, size(a.name) AS size"""

    val loadCsvUsingLinenumber =
      s"""LOAD CSV FROM '${csvDir}/artists.csv' AS line RETURN linenumber() as number, line"""

    val loadCsvUsingFile =
      s"""LOAD CSV FROM '${csvDir}/artists.csv' AS line RETURN DISTINCT file() as path"""

    println(loadCsv)
    db.cypher(loadCsv).show()

    println(loadCsvWithHeaders)
    db.cypher(loadCsvWithHeaders).show()

    println(loadCsvWithCustomDelimiter)
    db.cypher(loadCsvWithCustomDelimiter).show()

    println(loadCsvUsingPeriodicCommit)
    db.cypher(loadCsvUsingPeriodicCommit).show()

    println(loadCsvUsingPeriodicCommitSettingRate)
    db.cypher(loadCsvUsingPeriodicCommitSettingRate).show()

    println(loadCsvContainingEscapedCharacters)
    db.cypher(loadCsvContainingEscapedCharacters).show()

    println(loadCsvUsingLinenumber)
    db.cypher(loadCsvUsingLinenumber).show()

    println(loadCsvUsingFile)
    db.cypher(loadCsvUsingFile).show()
  }

}
