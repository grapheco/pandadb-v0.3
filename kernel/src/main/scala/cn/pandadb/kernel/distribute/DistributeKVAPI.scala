package cn.pandadb.kernel.distribute

import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import scala.collection.JavaConverters._
/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-12 11:06
 */

object DistributeKVAPI{
  def main(args: Array[String]): Unit = {
    val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
    val session = TiSession.create(conf)
    val client = new DistributeKVAPI(session.createRawClient())

  }
}

class DistributeKVAPI(client: RawKVClient) {
  implicit def arrayByte2TiKv(origin: Array[Byte]): ByteString = ByteString.copyFrom(origin)
  implicit def tiKv2ArrayByte(origin: ByteString): Array[Byte] = origin.toByteArray

  def put(key: Array[Byte], value: Array[Byte]): Unit = client.put(key, value)

  def get(key: Array[Byte]): Array[Byte] = client.get(key).toByteArray

  def delete(key: Array[Byte]): Unit = client.delete(key)

  def deletePrefix(key: Array[Byte]): Unit = client.deletePrefix(key)

  def deleteRange(startKey: Array[Byte], endKey: Array[Byte]): Unit = client.deleteRange(startKey, endKey)

  // scan.....
  def scan(startKey: Array[Byte], limit: Int): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scan(startKey, limit)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scan(startKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scan(startKey, limit, keyOnly)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scan(startKey: Array[Byte], endKey: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scan(startKey, endKey)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scan(startKey: Array[Byte], endKey: Array[Byte], keyOnly: Boolean): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scan(startKey, endKey, keyOnly)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scan(startKey: Array[Byte], endKey: Array[Byte], limit: Int): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scan(startKey, endKey, limit)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scan(startKey: Array[Byte], endKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scan(startKey, endKey, limit, keyOnly)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  // prefix...
  def scanPrefix(startKey: Array[Byte]): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scanPrefix(startKey)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scanPrefix(startKey: Array[Byte], keyOnly: Boolean): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scanPrefix(startKey, keyOnly)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }
  def scanPrefix(startKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[(Array[Byte], Array[Byte])] ={
    val iter = client.scanPrefix(startKey, limit, keyOnly)
    iter.iterator().asScala.map(kv => (kv.getKey, kv.getValue))
  }

  def batchGet(): Unit = {}
  def batchScan(): Unit = {}
  def batchDelete(): Unit = {}
  def batchPut(): Unit = {}
}
