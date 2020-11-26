package cn.pandadb.kernel.kv

import cn.pandadb.kernel.GraphRAM
import cn.pandadb.kernel.store.{StoredNode, StoredRelation}

class RocksDBGraphImpl extends GraphRAM {
  override def addNode(t: StoredNode): Unit = ???

  override def nodeAt(id: Id): StoredNode = ???

  //TODO: This function not support, no relation id in the kv solution.
  override def relationAt(id: Id): StoredRelation = ???

  override def deleteNode(id: Id): Unit = ???

  override def addRelation(t: StoredRelation): Unit = ???

  override def deleteRelation(id: Id): Unit = ???

  override def nodes(): Iterator[StoredNode] = ???

  override def rels(): Iterator[StoredRelation] = ???

  override def clear(): Unit = ???

  override def close(): Unit = ???

  // below is the code added by zhaozihao, for possible further use.
  def relsFrom(id: Id): Iterable[StoredRelation] = ???
  def relsTo(id: Id): Iterable[StoredRelation] = ???

  def SearchByLabel(label: Label): Iterable[StoredNode] = ???
  def SearchByType(t: Type): Iterable[StoredRelation] = ???

  def SearchByProperty(stat: Stat): Iterable[StoredNode] = ???

  //essential? could check whether index available in implemantion.
  def SearchByIndexedProperty(stat: Stat): Iterable[StoredNode] = ???
  def SearchByCategory(category: Category): Iterable[StoredRelation] = ???


}

case class Label(label: String)
case class Type(t: String)

case class Stat()
case class Category()