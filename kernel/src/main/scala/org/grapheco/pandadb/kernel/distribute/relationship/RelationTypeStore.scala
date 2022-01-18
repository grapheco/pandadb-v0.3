package org.grapheco.pandadb.kernel.distribute.relationship

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-18 10:52
 */

class RelationTypeStore(db: DistributedKVAPI) {
  implicit def ByteString2ArrayByte(data: ByteString) = data.toByteArray
  val BATCH_SIZE = 10000

  def set(labelId: Int, relId: Long): Unit = {
    val keyBytes = DistributedKeyConverter.toRelationTypeKey(labelId, relId)
    db.put(keyBytes, Array.emptyByteArray)
  }

  def delete(labelId: Int, relId: Long): Unit = {
    val keyBytes = DistributedKeyConverter.toRelationTypeKey(labelId, relId)
    db.delete(keyBytes)
  }

  def getRelationIds(labelId: Int): Iterator[Long] = {
    val keyPrefix = DistributedKeyConverter.toRelationTypeKey(labelId)
    val prefixLength = keyPrefix.length
    val iter = db.scanPrefix(keyPrefix, BATCH_SIZE, true)

    new Iterator[Long]() {
      override def hasNext: Boolean = iter.hasNext

      override def next(): Long = ByteUtils.getLong(iter.next().getKey, prefixLength)
    }
  }
}
