package cn.pandadb

import cn.pandadb.kernel.GraphDatabaseBuilder
import org.junit.{After, Assert, Before, Test}


class GraphDatabaseBuilder {
  @Test
  def createDBTest(): Unit = {
    val db = GraphDatabaseBuilder.newEmbeddedDatabase("testDB")
    Assert.assertNotNull(db)
    val n1 = db.addNode(Map("arr1"->"value1", "arr2"->12, "arr3"->12.5, "arr4"->true))
    db.close()
  }
}
