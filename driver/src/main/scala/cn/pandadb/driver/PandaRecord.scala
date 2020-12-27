package cn.pandadb.driver

import java.{util => javaUtil}

import org.neo4j.driver.types.{Entity, Node, Path, Relationship}
import org.neo4j.driver.{Record, Value, util}

class PandaRecord(names:String) extends Record {
  override def keys(): javaUtil.List[String] = ???

  override def values(): javaUtil.List[Value] = ???

  override def containsKey(key: String): Boolean = ???

  override def index(key: String): Int = ???

  override def get(key: String): Value = ???

  override def get(index: Int): Value = ???

  override def size(): Int = ???

  override def asMap(): javaUtil.Map[String, AnyRef] = ???

  override def asMap[T](mapper: javaUtil.function.Function[Value, T]): javaUtil.Map[String, T] = ???

  override def fields(): javaUtil.List[util.Pair[String, Value]] = ???

  override def get(key: String, defaultValue: Value): Value = ???

  override def get(key: String, defaultValue: Any): AnyRef = ???

  override def get(key: String, defaultValue: Number): Number = ???

  override def get(key: String, defaultValue: Entity): Entity = ???

  override def get(key: String, defaultValue: Node): Node = ???

  override def get(key: String, defaultValue: Path): Path = ???

  override def get(key: String, defaultValue: Relationship): Relationship = ???

  override def get(key: String, defaultValue: javaUtil.List[AnyRef]): javaUtil.List[AnyRef] = ???

  override def get[T](key: String, defaultValue: javaUtil.List[T], mapFunc: javaUtil.function.Function[Value, T]): javaUtil.List[T] = ???

  override def get(key: String, defaultValue: javaUtil.Map[String, AnyRef]): javaUtil.Map[String, AnyRef] = ???

  override def get[T](key: String, defaultValue: javaUtil.Map[String, T], mapFunc: javaUtil.function.Function[Value, T]): javaUtil.Map[String, T] = ???

  override def get(key: String, defaultValue: Int): Int = ???

  override def get(key: String, defaultValue: Long): Long = ???

  override def get(key: String, defaultValue: Boolean): Boolean = ???

  override def get(key: String, defaultValue: String): String = ???

  override def get(key: String, defaultValue: Float): Float = ???

  override def get(key: String, defaultValue: Double): Double = ???
}
