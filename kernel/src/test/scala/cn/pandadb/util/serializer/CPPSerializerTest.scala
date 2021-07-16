package cn.pandadb.util.serializer

import cn.pandadb.kernel.util.serializer.CPPSerializer
import org.junit.Test

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 1:40 下午 2021/6/8
 * @Modified By:
 */
class CPPSerializerTest {
  val serializer = new CPPSerializer()

  @Test
  def test1(): Unit = {
    System.loadLibrary("CP")
    val bytes: Array[Byte] = serializer.serialize(1, 2L)
    println(bytes)
  }

}
