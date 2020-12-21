package cn.pandadb.tools.importer

import java.io.File

import org.junit.Test

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 14:43 2020/12/19
 * @Modified By:
 */
class ImporterTest {
  @Test
  def test(): Unit = {
    val file = new File("G://dataset//head//nodes-1B-wrapped-head.csv")
    val importer = new Importer
    val cnt = importer.estLineCount(file)
    println(cnt)
  }
}
