package cn.pandadb.util.serializer

import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.Profiler.timing
import cn.pandadb.kernel.util.serializer.{NodeSerializer, PandaIteratorForValueDeSerializer}
import org.junit.{Assert, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 5:46 下午 2021/10/5
 * @Modified By:
 */

class PandaIteratorForValueDeSerializerTest {

  @Test
  def test1(): Unit = {
    val source: Array[Array[Byte]] = timing(prepareSource)
    val iter: Iterator[Array[Byte]] = source.toIterator
    println(s"plain deser")
    val result1 = timing(source.map(nodeInBytes => NodeSerializer.deserializeNodeValue(nodeInBytes)))

    println(s"parallel")
    val result2 = timing({
      val pandaIter = new PandaIteratorForValueDeSerializer[StoredNodeWithProperty](iter, stepLength = 1000000,NodeSerializer.deserializeNodeValue(_))
      pandaIter.toArray
    })

    result1.zip(result2).foreach(pair => Assert.assertEquals(pair._1.id, pair._2.id))
  }

  def prepareSource: Array[Array[Byte]] = {
    val nodeCount = 5000000

    val properties: Map[Int, Any] = Map(1->123, 2-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
      3-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
      4-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
      5-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
      6-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
      7-> "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")

    val nodeInBytes: Array[Array[Byte]] = (1 to nodeCount).map(i =>
    {
      val node = new StoredNodeWithProperty(i , Array(1,2,3), properties)
      NodeSerializer.serialize(node)
    }
    ).toArray
    nodeInBytes
  }

}
