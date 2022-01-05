package cn.pandadb.kv.distributed.store

import java.nio.ByteBuffer
import java.util

import cn.pandadb.kernel.distribute.{DistributedGraphFacade, DistributedKeyConverter}
import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.udp.UDPClient
import org.junit.Test
import org.tikv.common.types.Charset
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 10:44
 */
class Test1 {
  val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
  val session = TiSession.create(conf)
  val tikv: RawKVClient = session.createRawClient()
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))


  @Test
  def confTest(): Unit ={
    val c = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379").getTableScanConcurrency
    println(c)
  }

  @Test
  def clean(): Unit ={
    val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
    val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
    val db = new DistributedGraphFacade(kvHosts, indexHosts, udpClient)
    db.cleanDB()
  }

  @Test
  def deleteByPrefix(): Unit ={
    // todo
  }

  @Test
  def addData(): Unit ={
    tikv.put(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100, 1)), ByteString.copyFrom(Array.emptyByteArray))
    tikv.put(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100, 2)), ByteString.copyFrom(Array.emptyByteArray))
    tikv.put(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100, 3)), ByteString.copyFrom(Array.emptyByteArray))
  }

  @Test
  def deleteAll(): Unit = {
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }


  @Test
  def search(): Unit ={
    val iter = tikv.scanPrefix(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100))).iterator().asScala.map(f => f.getKey.toByteArray)
    iter.foreach(ba => println(DistributedKeyConverter.getLabelIdInNodeLabelKey(ba), DistributedKeyConverter.getNodeIdInNodeLabelKey(ba)))

  }
  @Test
  def testDeleteRange(): Unit ={
    val left = DistributedKeyConverter.toNodeLabelKey(100, 0)
    val right = DistributedKeyConverter.toNodeLabelKey(100, -1)

    tikv.deleteRange(ByteString.copyFrom(left), ByteString.copyFrom(right))
  }
  @Test
  def testBatchDelete(): Unit ={
    val keys = Array(
      DistributedKeyConverter.toNodeLabelKey(100, 1),
      DistributedKeyConverter.toNodeLabelKey(100, 2)
    ).map(f => ByteString.copyFrom(f))
    tikv.batchDelete(new util.ArrayList[ByteString](JavaConverters.seqAsJavaList(keys))) // transfer list to ArrayList
  }

  @Test
  def t(): Unit ={
    val lst = ArrayBuffer[(ByteString, ByteString)]()
    var count = 0
    for (i <- 1 to 500000){
      val kv = ByteString.copyFrom(ByteUtils.longToBytes(i))
      lst.append((kv, kv))
      count += 1
      if (count % 10000 == 0){
        println(s"data size: $count")
        val map = lst.toMap.asJava
        tikv.batchPut(map)
        lst.clear()
      }
    }
    tikv.close()
  }
  @Test
  def getALl(): Unit ={

//    println(iter.size())
    val start = ByteString.copyFrom(ByteUtils.longToBytes(0))
    val end = ByteString.copyFrom(ByteUtils.longToBytes(-1))
    println(tikv.scan(start, end).size())
  }
}
