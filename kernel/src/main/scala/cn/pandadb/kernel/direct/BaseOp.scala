package cn.pandadb.kernel.direct

import cn.pandadb.kernel.store.StoredRelation

trait BasicOp {
  def get(relationId: Long): StoredRelation
  def delete(relationId: Long): Unit
  def update(r:StoredRelation): Unit
  def put(r:StoredRelation): Unit
  def size(): Int
  def capacity(): Int
}
