package cn.pandadb

import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import org.junit.{Assert, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:13 2021/5/6
 * @Modified By:
 */
class PandaDBExceptionTest {
  @Test
  def test1(): Unit = {
    val exception = new PandaDBException("test")
    Assert.assertEquals("test", exception.getMessage)
  }

}
