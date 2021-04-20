package cn.pandadb

import cn.pandadb.kernel.GraphDatabaseBuilder
import org.junit.{After, Assert, Before, Test}


class GraphDatabaseBuilderTest {
  @Test
  def createDBTest(): Unit = {
    var hasError: Boolean = false
    try {
      val db = GraphDatabaseBuilder.newEmbeddedDatabase("testData")
      Assert.assertNotNull(db)
      val n1 = db.addNode(Map("arr1" -> "value1", "arr2" -> 12, "arr3" -> 12.5, "arr4" -> true))
      db.close()
      val db2 = GraphDatabaseBuilder.newEmbeddedDatabase("testData")
      Assert.assertNotNull(db2)
      db2.close()
    }catch {
      case e: Exception => hasError = true
    }

    Assert.assertFalse(hasError)
  }
}
