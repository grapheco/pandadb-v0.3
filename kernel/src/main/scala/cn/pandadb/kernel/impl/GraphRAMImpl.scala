package scala.cn.pandadb.kernel.impl

import cn.pandadb.kernel.GraphRAM
import cn.pandadb.kernel.direct.{DirectBufferArray, DirectBufferArrayForNode}
import cn.pandadb.kernel.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class GraphRAMImpl extends GraphRAM {

  val nodeStorage = new DirectBufferArrayForNode(1024 * 1024 * 1024 * 4, 8)
  val relationsStorage = new DirectBufferArray(1024 * 1024 * 1024 * 4, 8 * 3 + 4)

  override def addNode(t: StoredNode): Unit = nodeStorage.put(t)

  override def deleteNode(id: Id): Unit = nodeStorage.delete(id)

  override def addRelation(t: StoredRelation): Unit = relationsStorage.put(t)

  override def deleteRelation(id: Id): Unit = relationsStorage.delete(id)

  override def nodes(): Seq[StoredNode] = nodeStorage.iterator.toSeq

  override def rels(): Seq[StoredRelation] = relationsStorage.iterator.toSeq

  override def close(): Unit = {
    clear()
  }

  override def clear(): Unit = {
    nodeStorage.clear()
    relationsStorage.clear()
  }

  override def nodeAt(id: Id): StoredNode = nodeStorage.get(id)

  override def relationAt(id: Id): StoredRelation = relationsStorage.get(id)
}
