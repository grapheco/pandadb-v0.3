package cn.pandadb.kernel.distribute

import java.util

import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter}
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.tikv.common.types.Charset
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.kvproto.Kvrpcpb
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-12 11:06
 */

trait DistributedKVAPI {
  def put(key: Array[Byte], value: Array[Byte]): Unit
  def get(key: Array[Byte]): Array[Byte]
  def delete(key: Array[Byte]): Unit
  def deletePrefix(key: Array[Byte]): Unit
  def deleteRange(startKey: Array[Byte], endKey: Array[Byte]): Unit

  def scan(startKey: Array[Byte], limit: Int): Iterator[Kvrpcpb.KvPair]
  def scan(startKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair]
  def scan(startKey: Array[Byte], endKey: Array[Byte]): Iterator[Kvrpcpb.KvPair]
  def scan(startKey: Array[Byte], endKey: Array[Byte], limit: Int): Iterator[Kvrpcpb.KvPair]
  def scan(startKey: Array[Byte], endKey: Array[Byte], keyOnly: Boolean): Iterator[Kvrpcpb.KvPair]
  def scan(startKey: Array[Byte], endKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair]
  def scanPrefix(startKey: Array[Byte]): Iterator[Kvrpcpb.KvPair]
  def scanPrefix(startKey: Array[Byte], keyOnly: Boolean): Iterator[Kvrpcpb.KvPair]
  def scanPrefix(startKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair]

  def batchGetValue(keys: Seq[Array[Byte]]): Iterator[Array[Byte]]
  def batchScan(): Iterator[(Array[Byte], Array[Byte])]
  def batchDelete(data: Seq[Array[Byte]]): Unit
  def batchPut(kvParis: Seq[(Array[Byte], Array[Byte])]): Unit
}

object DistributedKVAPI{
  def main(args: Array[String]): Unit = {
    val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
    val session = TiSession.create(conf)
    val client = new PandaDistributeKVAPI(session.createRawClient())

//    val k1 = KeyConverter.toNodeKey(1, 1)
//    val k2 = KeyConverter.toNodeKey(1, 2)
//    val k3 = KeyConverter.toNodeKey(1, 3)
//
//    client.put(k1, "a".getBytes(Charset.CharsetUTF8))
//    client.put(k2, "b".getBytes(Charset.CharsetUTF8))
//    client.put(k3, "c".getBytes(Charset.CharsetUTF8))
    val res = client.get("cc".getBytes("UTF-8"))
    res.foreach(println)
  }
}

class PandaDistributeKVAPI(client: RawKVClient) extends DistributedKVAPI {
  implicit def arrayByte2TiKv(origin: Array[Byte]): ByteString = ByteString.copyFrom(origin)
  implicit def tiKv2ArrayByte(origin: ByteString): Array[Byte] = origin.toByteArray

  override def put(key: Array[Byte], value: Array[Byte]): Unit = client.put(key, value)

  override def get(key: Array[Byte]): Array[Byte] = client.get(key).toByteArray

  override def delete(key: Array[Byte]): Unit = client.delete(key)

  override def deletePrefix(key: Array[Byte]): Unit = client.deletePrefix(key)

  override def deleteRange(startKey: Array[Byte], endKey: Array[Byte]): Unit = client.deleteRange(startKey, endKey)

  // scan.....
  override def scan(startKey: Array[Byte], limit: Int): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scan(startKey, limit)
    iter.iterator().asScala
  }
  override def scan(startKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scan(startKey, limit, keyOnly)
    iter.iterator().asScala
  }
  override def scan(startKey: Array[Byte], endKey: Array[Byte]): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scan(startKey, endKey)
    iter.iterator().asScala
  }
  override def scan(startKey: Array[Byte], endKey: Array[Byte], keyOnly: Boolean): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scan(startKey, endKey, keyOnly)
    iter.iterator().asScala
  }
  override def scan(startKey: Array[Byte], endKey: Array[Byte], limit: Int): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scan(startKey, endKey, limit)
    iter.iterator().asScala
  }
  override def scan(startKey: Array[Byte], endKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scan(startKey, endKey, limit, keyOnly)
    iter.iterator().asScala
  }
  // prefix...
  override def scanPrefix(startKey: Array[Byte]): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scanPrefix(startKey)
    iter.iterator().asScala
  }
  override def scanPrefix(startKey: Array[Byte], keyOnly: Boolean): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scanPrefix(startKey, keyOnly)
    iter.iterator().asScala
  }
  override def scanPrefix(startKey: Array[Byte], limit: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair] ={
    val iter = client.scanPrefix(startKey, limit, keyOnly)
    iter.iterator().asScala
  }

  override def batchGetValue(keys: Seq[Array[Byte]]): Iterator[Array[Byte]] = {
    val _keys = JavaConverters.seqAsJavaList(keys.map(ByteString.copyFrom(_)))
    client.batchGet(new util.ArrayList[ByteString](_keys)).iterator().asScala.map(kv => kv.getValue)
  }
  override def batchScan(): Iterator[(Array[Byte], Array[Byte])] = ???

  override def batchDelete(data: Seq[Array[Byte]]): Unit = {
    val transfer = new util.ArrayList[ByteString](JavaConverters.seqAsJavaList(data.map(f => ByteString.copyFrom(f))))
    client.batchDelete(transfer)
  }
  override def batchPut(kvParis: Seq[(Array[Byte], Array[Byte])]): Unit = {
    val map = kvParis.map(kv => (ByteString.copyFrom(kv._1), ByteString.copyFrom(kv._2))).toMap.asJava
    client.batchPut(map)
  }
}
