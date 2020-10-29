package cn.pandadb.pnode.impl

import java.nio.ByteBuffer

trait BasicOp {
  def get(relationId: Long): (Int, (Long, Long, Long, Long))
  def delete(relationId: Long): Unit
  def update(relationId: Long, typeId: Long, from: Long, to: Long): Unit
  def put(relationId: Long, typeId: Long, from: Long, to: Long): Unit
  def size(): Int
  def capacity(): Int
}
