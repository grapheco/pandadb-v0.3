package org.grapheco.pandadb.tools.importer

import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created in 10:30 2021/4/28
 * @Modified By:
 */
class CSVIOTest {

  @Test
  def test1(): Unit = {
    val line = "1,,3"
    val csvLine = new CSVLine(line.split(","))
    Array("1", "", "3").zip(csvLine.getAsArray).foreach(pair => Assert.assertEquals(pair._1, pair._2))
  }

  @Test
  def test2(): Unit = {
    val line = "1,,3,[]"
    val csvLine = new CSVLine(line.split(","))
    Array("1", "", "3", "[]").zip(csvLine.getAsArray).foreach(pair => Assert.assertEquals(pair._1, pair._2))
  }

  @Test
  def test3(): Unit = {
    val line = ",,"
    val csvLine = new CSVLine(line.split(","))
    Array("", "", "").zip((csvLine.getAsArray)).foreach(pair => Assert.assertEquals(pair._1, pair._2))

  }

}
