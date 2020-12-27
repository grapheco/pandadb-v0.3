import cn.pandadb.driver.PandaDriver
import org.junit.{Before, Test}

class DriverTest {


  @Test
  def test(): Unit ={
    val driver = new PandaDriver
    val session = driver.session()
    val res = session.run("match (n) return n")
    session.close()
    println(res.keys())
  }

}
