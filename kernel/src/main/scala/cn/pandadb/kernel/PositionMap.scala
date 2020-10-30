package cn.pandadb.kernel

import scala.collection.mutable

trait PositionMap {
  type Id = Long
  type Position = Long

  def update(id: Id, pos: Position)

  def position(id: Id): Option[Position]

  def clear(): Unit
}

class SimplePositionMap extends PositionMap {
  val posMap = mutable.LinkedHashMap[Id, Position]()

  override def update(id: Id, pos: Position): Unit = posMap += id -> pos

  override def position(id: Id): Option[Position] = posMap.get(id)

  override def clear(): Unit = posMap.clear()
}
