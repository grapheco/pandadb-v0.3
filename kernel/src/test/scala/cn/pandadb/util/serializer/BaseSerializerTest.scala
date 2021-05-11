package cn.pandadb.util.serializer

import cn.pandadb.kernel.blob.api.{Blob, ManagedBlob}
import cn.pandadb.kernel.blob.impl.BlobFactory
import cn.pandadb.kernel.util.Profiler.timing
import cn.pandadb.kernel.util.serializer.BaseSerializer
import io.netty.buffer.{ByteBuf, UnpooledByteBufAllocator, UnpooledHeapByteBuf}
import org.junit.{Assert, Test}

import java.io.File
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 21:14 2020/12/17
 * @Modified By:
 */
class BaseSerializerTest {
  val serializer = BaseSerializer

  val arr: Array[Int] = Array(1, 2, 3)
  val map: Map[Int, Any] = Map(1 -> 1, 2 -> "two", 3 -> true, 4 -> 4.0)

  @Test
  def testArr(): Unit = {
    val bytesArr = serializer.array2Bytes(arr)
    Assert.assertArrayEquals(arr, serializer.bytes2Array(bytesArr).asInstanceOf[Array[Int]])
    println("serialize arr.")
    timing(for (i<-1 to 10000000) serializer.array2Bytes(arr))
    println("deserialize arr")
    timing(for (i<-1 to 10000000) serializer.bytes2Array(bytesArr))
  }

  @Test
  def testMap(): Unit = {
    val bytesArr = serializer.map2Bytes(map)
    val mMap = serializer.bytes2Map(bytesArr)
    Assert.assertEquals(map, mMap)
    println("serialize map.")
    timing(for (i<-1 to 10000000) serializer.map2Bytes(map))
    println("deserialize map.")
    timing(for (i<-1 to 10000000) serializer.bytes2Map(bytesArr))
  }

  @Test
  def arrayInMap(): Unit = {
    val array1 = Array(1,2,3)
    val array2 = Array(100L, 200L, 300L)
    val array3 = Array(1.0, 2.0, 3.0)
    val array4 = Array("abc", "123")
    val array5 = Array(true, false)
    val array6 = Array(1.0f, 2.0f, 3.0f)
    val map = Map(1->array1, 2->array2, 3->array3, 4->array4, 5->array5, 6->array6)
    val bytesArr = serializer.map2Bytes(map)
    val mMap = serializer.bytes2Map(bytesArr)

    _arrayContentEquals(array1, mMap(1).asInstanceOf[Array[Int]])
    _arrayContentEquals(array2, mMap(2).asInstanceOf[Array[Long]])
    _arrayContentEquals(array3, mMap(3).asInstanceOf[Array[Double]])
    _arrayContentEquals(array4, mMap(4).asInstanceOf[Array[String]])
    _arrayContentEquals(array5, mMap(5).asInstanceOf[Array[Boolean]])
    _arrayContentEquals(array6, mMap(6).asInstanceOf[Array[Float]])
  }

  @Test
  def testLongString(): Unit = {
    val testString = Source.fromFile("./testinput/longString").mkString
    val map = Map(1 -> testString)
    val bytesArr = serializer.map2Bytes(map)
    val deserializedMap = serializer.bytes2Map(bytesArr)
    Assert.assertEquals(testString, deserializedMap(1))
  }

  def _arrayContentEquals(array1: Array[_], array2: Array[_]): Unit = {
    array1.zip(array2).foreach(pair => Assert.assertEquals(pair._1, pair._2))
  }

  @Test
  def testBlob(): Unit = {
    val surl = "https://www.baidu.com/img/flexible/logo/pc/result.png"
    val blob = BlobFactory.fromHttpURL(surl)
    val bytesArray = BaseSerializer.map2Bytes(Map(1-> blob))
    val deSerializedBlob = BaseSerializer.bytes2Map(bytesArray).get(1).get.asInstanceOf[Blob]
    Assert.assertArrayEquals(blob.toBytes(), deSerializedBlob.toBytes())
  }

}
