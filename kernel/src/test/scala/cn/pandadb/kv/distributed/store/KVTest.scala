package cn.pandadb.kv.distributed.store

import java.util

import cn.pandadb.kernel.distribute.DistributedKeyConverter
import cn.pandadb.kernel.kv.ByteUtils
import org.junit.Test
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import scala.collection.JavaConverters
import scala.collection.JavaConverters._

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

  @Test
  def addData(): Unit ={
    tikv.put(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100, 1)), ByteString.copyFrom(Array.emptyByteArray))
    tikv.put(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100, 2)), ByteString.copyFrom(Array.emptyByteArray))
    tikv.put(ByteString.copyFrom(DistributedKeyConverter.toNodeLabelKey(100, 3)), ByteString.copyFrom(Array.emptyByteArray))
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

    println("")
  }
}
