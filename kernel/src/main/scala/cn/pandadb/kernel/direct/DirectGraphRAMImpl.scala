package cn.pandadb.kernel.direct

import cn.pandadb.kernel.GraphRAM
import cn.pandadb.kernel.store.{StoredNode, StoredRelation}

class DirectGraphRAMImpl extends GraphRAM {

  override def addNode(t: StoredNode): Unit = ???

  override def nodeAt(id: Id): StoredNode = ???

  override def relationAt(id: Id): StoredRelation = ???

  override def deleteNode(id: Id): Unit = ???

  override def addRelation(t: StoredRelation): Unit = ???

  override def deleteRelation(id: Id): Unit = ???

  override def nodes(): Seq[StoredNode] = ???

  override def rels(): Seq[StoredRelation] = ???

  override def clear(): Unit = ???

  override def close(): Unit = ???
}
