import cn.pandadb.driver.PandaDriver
import cn.pandadb.driver.utils.Types
import org.junit.Test
import org.neo4j.driver.internal.value.RelationshipValue
import org.neo4j.driver.types.Relationship

class TestDriver {
  @Test
  def test(): Unit ={
    val driver = new PandaDriver
    val session = driver.session()
    val res = session.run("match (n) return n, n.name")
    while (res.hasNext){
      println(res.next())
    }
    session.close()
    driver.close()
  }
}
