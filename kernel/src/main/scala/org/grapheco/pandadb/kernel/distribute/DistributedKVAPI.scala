package org.grapheco.pandadb.kernel.distribute

import org.tikv.common.util.ScanOption

import java.util
import java.util.List
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

  def scanPrefix(prefix: Array[Byte], batchSize: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair]

  def batchGetValue(keys: Seq[Array[Byte]]): Iterator[Array[Byte]]

  def batchScan(list: util.List[ScanOption]): util.List[util.List[Kvrpcpb.KvPair]]

  def batchDelete(data: Seq[Array[Byte]]): Unit

  def batchPut(kvParis: Seq[(Array[Byte], Array[Byte])]): Unit

  def close(): Unit
}

class PandaDistributeKVAPI(client: RawKVClient) extends DistributedKVAPI {

  implicit def arrayByte2TiKv(origin: Array[Byte]): ByteString = ByteString.copyFrom(origin)

  implicit def tiKv2ArrayByte(origin: ByteString): Array[Byte] = origin.toByteArray

  override def put(key: Array[Byte], value: Array[Byte]): Unit = client.put(key, value)

  override def get(key: Array[Byte]): Array[Byte] = client.get(key).toByteArray

  override def delete(key: Array[Byte]): Unit = client.delete(key)

  override def deletePrefix(key: Array[Byte]): Unit = client.deletePrefix(key)

  override def deleteRange(startKey: Array[Byte], endKey: Array[Byte]): Unit = client.deleteRange(startKey, endKey)

  override def scanPrefix(prefix: Array[Byte], batchSize: Int, keyOnly: Boolean): Iterator[Kvrpcpb.KvPair] = {
    val iter = new Iterator[Seq[Kvrpcpb.KvPair]] {
      var nextData: Seq[Kvrpcpb.KvPair] = _
      var lastKey: ByteString = _
      var isFirst = true
      var notFinish = true

      override def hasNext: Boolean = {
        if (notFinish){
          if (isFirst) nextData = client.scanPrefix(prefix, batchSize, keyOnly).asScala
          else nextData = client.scan(lastKey, batchSize, keyOnly).asScala.filter(p => p.getKey.startsWith(prefix))

          if (nextData.nonEmpty){
            lastKey = nextData.last.getKey
            true
          }
          else false
        }
        else false
      }
      override def next(): Seq[Kvrpcpb.KvPair] = {
        val data = {
          if (isFirst) {
            isFirst = false
            nextData
          }
          else nextData.tail
        }
        if (data.nonEmpty) data
        else {
          notFinish = false
          Seq.empty
        }
      }
    }
    iter.flatten
  }

  override def batchGetValue(keys: Seq[Array[Byte]]): Iterator[Array[Byte]] = {
    val _keys = JavaConverters.seqAsJavaList(keys.map(ByteString.copyFrom(_)))
    client.batchGet(new util.ArrayList[ByteString](_keys)).iterator().asScala.map(kv => kv.getValue)
  }

  override def batchScan(ranges: util.List[ScanOption]): util.List[util.List[Kvrpcpb.KvPair]] = client.batchScan(ranges)

  override def batchDelete(data: Seq[Array[Byte]]): Unit = {
    val transfer = new util.ArrayList[ByteString](JavaConverters.seqAsJavaList(data.map(f => ByteString.copyFrom(f))))
    client.batchDelete(transfer)
  }

  override def batchPut(kvParis: Seq[(Array[Byte], Array[Byte])]): Unit = {
    val map = kvParis.map(kv => (ByteString.copyFrom(kv._1), ByteString.copyFrom(kv._2))).toMap.asJava
    client.batchPut(map)
  }

  override def close(): Unit = client.close()
}
