package cn.pandadb.kernel.store

trait XStore[Id, T] {
  def close(): Unit

  def loadAll(): Seq[T]

  def update(t: T): Unit

  def delete(id: Id): Unit

  def updateAll(ts: Seq[T]): Unit = ts.foreach(update(_))

  def deleteAll(ts: Seq[Id]): Unit = ts.foreach(delete(_))

  def saveAll(ts: Seq[T])
}

trait NodeStore extends XStore[Long, StoredNode] {
}

trait RelationStore extends XStore[Long, StoredRelation] {
}