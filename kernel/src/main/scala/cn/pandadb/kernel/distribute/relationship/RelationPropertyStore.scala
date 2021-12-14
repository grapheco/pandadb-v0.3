package cn.pandadb.kernel.distribute.relationship

import cn.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import cn.pandadb.kernel.store.{StoredRelation, StoredRelationWithProperty}
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-18 11:15
 */
class RelationPropertyStore(db: DistributedKVAPI) {
  implicit def ByteString2ArrayByte(data: ByteString) = data.toByteArray
  val BATCH_SIZE = 10000

  def set(relation: StoredRelationWithProperty): Unit = {
    val keyBytes = DistributedKeyConverter.toRelationKey(relation.id)
    db.put(keyBytes, RelationSerializer.serialize(relation))
  }

  def set(relation: StoredRelation): Unit = {
    val keyBytes = DistributedKeyConverter.toRelationKey(relation.id)
    db.put(keyBytes, RelationSerializer.serialize(
      new StoredRelationWithProperty(relation.id, relation.from, relation.to, relation.typeId, Map())))
  }

  def delete(relationId: Long): Unit = db.delete(DistributedKeyConverter.toRelationKey(relationId))

  def get(relationId: Long): Option[StoredRelationWithProperty] = {
    val keyBytes = DistributedKeyConverter.toRelationKey(relationId)
    val res = db.get(keyBytes)
    if (res.nonEmpty) Some(RelationSerializer.deserializeRelWithProps(res))
    else None
  }

  def exist(relationId: Long): Boolean = db.get(DistributedKeyConverter.toRelationKey(relationId)).nonEmpty

  def all(): Iterator[StoredRelationWithProperty] = {
    new Iterator[StoredRelationWithProperty]{
      val iter = db.scanPrefix(Array(DistributedKeyConverter.relationKeyPrefix), BATCH_SIZE, false)

      override def hasNext: Boolean = iter.hasNext

      override def next(): StoredRelationWithProperty = RelationSerializer.deserializeRelWithProps(iter.next().getValue)
    }
  }
  def count: Long = {
    val iter = db.scanPrefix(Array(DistributedKeyConverter.relationKeyPrefix), BATCH_SIZE, true)
    var count:Long = 0
    while (iter.hasNext){
      count+=1
      iter.next()
    }
    count
  }
}
