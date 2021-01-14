package cn.pandadb.driver


import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime, ZonedDateTime}
import java.{lang, util => javaUtil}

import org.neo4j.driver.internal.util.Format.formatPairs
import cn.pandadb.driver.utils.{Types, TypesToNeo4jValue}
import cn.pandadb.hipporpc.utils.DriverValue
import cn.pandadb.hipporpc.values.{Value => HippoValue}
import org.neo4j.driver.internal.value.BooleanValue
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.types.{Entity, IsoDuration, Node, Path, Point, Relationship, Type}
import org.neo4j.driver.{Record, Result, Value, util}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PandaStatementResult(driverStreamRecords: Stream[DriverValue], cypher: String, params:Map[String,Any]) extends Result {
  val metadata = driverStreamRecords.head
  val resultIterator = driverStreamRecords.tail.iterator

  override def keys(): javaUtil.List[String] = {
    seqAsJavaList(metadata.rowMap.keySet.toList)
  }

  override def hasNext: Boolean = {
    resultIterator.hasNext
  }

  override def next(): Record = {
   val driverValue = resultIterator.next()
    new Record {
      override def keys(): javaUtil.List[String] = seqAsJavaList(driverValue.rowMap.keySet.toList)

      override def values(): javaUtil.List[Value] = {
        val values = driverValue.rowMap.values.toIterator
        val list = new ArrayBuffer[Value]()
        while (values.hasNext){
          val row = values.next()
          list += TypesToNeo4jValue.getNeo4jValue(row)
        }
        seqAsJavaList(list)
      }

      override def containsKey(s: String): Boolean = driverValue.rowMap.keySet.contains(s)

      override def index(s: String): Int = ???

      override def get(s: String): Value = TypesToNeo4jValue.getNeo4jValue(driverValue.rowMap(s))

      override def get(i: Int): Value = TypesToNeo4jValue.getNeo4jValue(driverValue.rowMap.toList(i)._2)

      override def size(): Int = driverValue.rowMap.size

      override def asMap(): javaUtil.Map[String, AnyRef] = {
        val map = mutable.Map[String, AnyRef]()
        val keyList = keys()
        keyList.forEach(s => {
          val neo4jValue = get(s)
          map.put(s, neo4jValue)
        })
        mapAsJavaMap(map.toMap)
      }

      override def asMap[T](function: javaUtil.function.Function[Value, T]): javaUtil.Map[String, T] = ???

      override def fields(): javaUtil.List[util.Pair[String, Value]] = ???

      override def get(s: String, value: Value): Value = ???

      override def get(s: String, o: Any): AnyRef = ???

      override def get(s: String, number: Number): Number = ???

      override def get(s: String, entity: Entity): Entity = ???

      override def get(s: String, node: Node): Node = ???

      override def get(s: String, path: Path): Path = ???

      override def get(s: String, relationship: Relationship): Relationship = ???

      override def get(s: String, list: javaUtil.List[AnyRef]): javaUtil.List[AnyRef] = ???

      override def get[T](s: String, list: javaUtil.List[T], function: javaUtil.function.Function[Value, T]): javaUtil.List[T] = ???

      override def get(s: String, map: javaUtil.Map[String, AnyRef]): javaUtil.Map[String, AnyRef] = ???

      override def get[T](s: String, map: javaUtil.Map[String, T], function: javaUtil.function.Function[Value, T]): javaUtil.Map[String, T] = ???

      override def get(s: String, i: Int): Int = ???

      override def get(s: String, l: Long): Long = ???

      override def get(s: String, b: Boolean): Boolean = ???

      override def get(s: String, s1: String): String = ???

      override def get(s: String, v: Float): Float = ???

      override def get(s: String, v: Double): Double = ???

      override def toString: String = {
        String.format(s"Record<${formatPairs(asMap())}>")
      }
    }
  }

  override def single(): Record = ???

  override def peek(): Record = ???

  override def stream(): javaUtil.stream.Stream[Record] = {
    val spliterator = javaUtil.Spliterators.spliteratorUnknownSize(this, javaUtil.Spliterator.IMMUTABLE | javaUtil.Spliterator.ORDERED)
    javaUtil.stream.StreamSupport.stream(spliterator, false)
  }

  override def list(): javaUtil.List[Record] = ???

  override def list[T](mapFunction: javaUtil.function.Function[Record, T]): javaUtil.List[T] = ???

  override def consume(): ResultSummary = ???

}
