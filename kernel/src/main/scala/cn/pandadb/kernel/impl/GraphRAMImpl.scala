package scala.cn.pandadb.kernel.impl

import cn.pandadb.kernel.GraphRAM
import cn.pandadb.kernel.direct.DirectBufferArray
import cn.pandadb.kernel.store.{StoredNode, StoredRelation}

import scala.collection.mutable

class GraphRAMImpl extends GraphRAM {

  val mapNodes = mutable.LinkedHashMap[Id, StoredNode]()
  val relationsStorage = new DirectBufferArray(1024, 8 * 3 + 4)

  override def addNode(t: StoredNode): Unit = mapNodes += t.id -> t

  override def deleteNode(id: Id): Unit = mapNodes -= id

  override def addRelation(t: StoredRelation): Unit = relationsStorage.put(t)

  override def deleteRelation(id: Id): Unit = relationsStorage.delete(id)

  override def nodes(): Seq[StoredNode] = mapNodes.map(_._2).toSeq

  override def rels(): Seq[StoredRelation] = relationsStorage.iterator.toSeq

  override def close(): Unit = {
    clear()
  }

  override def clear(): Unit = {
    mapNodes.clear()
    relationsStorage.clear()
  }

  override def nodeAt(id: Id): StoredNode = mapNodes(id)

  override def relationAt(id: Id): StoredRelation = relationsStorage.get(id)
}
