package cn.pandadb.util.serializer

import cn.pandadb.kernel.util.Profiler.timing
import cn.pandadb.kernel.util.serializer.BaseSerializer
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
    val bytesArr = serializer.intArray2Bytes(arr)
    Assert.assertArrayEquals(arr, serializer.bytes2IntArray(bytesArr))
    println("serialize arr.")
    timing(for (i<-1 to 10000000) serializer.intArray2Bytes(arr))
    println("deserialize arr")
    timing(for (i<-1 to 10000000) serializer.bytes2IntArray(bytesArr))
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
  def testIntArrayMap(): Unit = {
    val bytesArr = serializer.intArrayMap2Bytes(arr, map)
    val result = serializer.bytes2IntArrayMap(bytesArr)
    Assert.assertArrayEquals(arr, result._1)
    Assert.assertEquals(map, result._2)
    println("serialize (int array, map).")
    timing(for(i<-1 to 10000000) serializer.intArrayMap2Bytes(arr, map))
    println("deserialize (int array, map).")
    timing(for(i<-1 to 10000000) serializer.bytes2IntArrayMap(bytesArr))
  }

  @Test
  def testLongString(): Unit = {
    println(Short.MaxValue)
    val testString = Source.fromFile("./testinput/longString").mkString
    val map = Map(1 -> testString)
    val bytesArr = serializer.map2Bytes(map)
    val deserializedMap = serializer.bytes2Map(bytesArr)
    Assert.assertEquals(testString, deserializedMap(1))
  }

}
