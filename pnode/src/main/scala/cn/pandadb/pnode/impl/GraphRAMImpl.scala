package scala.cn.pandadb.pnode.impl

import cn.pandadb.pnode.GraphRAM
import cn.pandadb.pnode.store.{StoredNode, StoredRelation}

class GraphRAMImpl extends GraphRAM {

  override def addNode(t: StoredNode): Unit = ???

  override def nodeAt(id: Id): StoredNode = ???

  override def relationAt(id: Id): StoredRelation = ???

  override def deleteNode(id: Id): Unit = ???

  override def addRelation(t: StoredRelation): Unit = ???

  override def deleteRelation(id: Id): Unit = ???

  override def nodes(): Stream[StoredNode] = ???

  override def rels(): Stream[StoredRelation] = ???

  override def clear(): Unit = ???

  override def close(): Unit = ???
}
