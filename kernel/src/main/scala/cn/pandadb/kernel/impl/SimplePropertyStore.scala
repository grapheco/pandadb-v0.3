package cn.pandadb.kernel.impl

import cn.pandadb.kernel.{PropertyStore, TypedId}

import scala.collection.mutable

class SimplePropertyStore extends PropertyStore {
  val propStore = mutable.Map[TypedId, mutable.Map[String, Any]]()

  override def insert(id: TypedId, props: Map[String, Any]): Unit =
    propStore += id -> (mutable.Map[String, Any]() ++ props)

  override def delete(id: TypedId): Unit = propStore -= id

  override def lookup(id: TypedId): Option[Map[String, Any]] = propStore.get(id).map(_.toMap)

  override def close(): Unit = {
  }
}
