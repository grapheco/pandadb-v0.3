package cn.pandadb.driver

import java.util.concurrent.TimeUnit
import java.{util => javaUtil}

import cn.pandadb.NotImplementMethodException
import cn.pandadb.driver.utils.TypesToNeo4jValue
import cn.pandadb.hipporpc.utils.DriverValue
import org.neo4j.driver.internal.util.Format.formatPairs
import org.neo4j.driver.internal.util.Futures
import org.neo4j.driver.v1.summary.{Notification, Plan, ProfiledPlan, ResultSummary, ServerInfo, StatementType, SummaryCounters}
import org.neo4j.driver.v1.types.{Entity, Node, Path, Relationship}
import org.neo4j.driver.v1.{Record, Statement, StatementResult, Value, util}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

class PandaStatementResult(driverStreamRecords: Stream[DriverValue], cypher: String, params:Map[String,Any], receiveDataHeadTime: Long, uriAddress:String) extends StatementResult  {
  val metadata = driverStreamRecords.head
  val resultIterator = driverStreamRecords.tail.iterator

  val resultConsumedBeginTime: Long = System.currentTimeMillis()
  var resultConsumedAfterTime: Long = _
  var count = 1

  override def keys(): javaUtil.List[String] = {
    seqAsJavaList(metadata.rowMap.keySet.toList)
  }

  override def hasNext: Boolean = {
    val flag = resultIterator.hasNext
    if (!flag && count == 1) {
      resultConsumedAfterTime = System.currentTimeMillis() - resultConsumedBeginTime
      count -= 1
    }
    flag
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

      override def asMap[T](function: util.Function[Value, T]): javaUtil.Map[String, T] = throw new NotImplementMethodException("asMap[T](function: util.Function[Value, T])")

      override def fields(): javaUtil.List[util.Pair[String, Value]] = throw new NotImplementMethodException("fields")

      override def get[T](s: String, list: javaUtil.List[T], function: util.Function[Value, T]): javaUtil.List[T] = throw new NotImplementMethodException("get[T](s: String, list: javaUtil.List[T], function: util.Function[Value, T])")

      override def get[T](s: String, map: javaUtil.Map[String, T], function: util.Function[Value, T]): javaUtil.Map[String, T] = throw new NotImplementMethodException("get[T](s: String, map: javaUtil.Map[String, T], function: util.Function[Value, T])")

      override def containsKey(s: String): Boolean = driverValue.rowMap.keySet.contains(s)

      override def index(s: String): Int = throw new NotImplementMethodException("index")

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

      override def get(s: String, value: Value): Value = throw new NotImplementMethodException("get(s: String, value: Value)")

      override def get(s: String, o: Any): AnyRef = throw new NotImplementMethodException("get(s: String, o: Any)")

      override def get(s: String, number: Number): Number = throw new NotImplementMethodException("get(s: String, number: Number)")

      override def get(s: String, entity: Entity): Entity = throw new NotImplementMethodException("get(s: String, entity: Entity)")

      override def get(s: String, node: Node): Node = throw new NotImplementMethodException("get(s: String, node: Node)")

      override def get(s: String, path: Path): Path = throw new NotImplementMethodException("get(s: String, path: Path)")

      override def get(s: String, relationship: Relationship): Relationship = throw new NotImplementMethodException("get(s: String, relationship: Relationship)")

      override def get(s: String, list: javaUtil.List[AnyRef]): javaUtil.List[AnyRef] = throw new NotImplementMethodException("get(s: String, list: javaUtil.List[AnyRef])")

      override def get(s: String, map: javaUtil.Map[String, AnyRef]): javaUtil.Map[String, AnyRef] = throw new NotImplementMethodException("get(s: String, map: javaUtil.Map[String, AnyRef])")

      override def get(s: String, i: Int): Int = throw new NotImplementMethodException("get(s: String, i: Int)")

      override def get(s: String, l: Long): Long = throw new NotImplementMethodException("get(s: String, l: Long)")

      override def get(s: String, b: Boolean): Boolean = throw new NotImplementMethodException("get(s: String, b: Boolean)")

      override def get(s: String, s1: String): String = throw new NotImplementMethodException("get(s: String, s1: String)")

      override def get(s: String, v: Float): Float = throw new NotImplementMethodException("get(s: String, v: Float)")

      override def get(s: String, v: Double): Double = throw new NotImplementMethodException("get(s: String, v: Double)")

      override def toString: String = {
        String.format(s"Record<${formatPairs(asMap())}>")
      }
    }
  }

  override def single(): Record = throw new NotImplementMethodException("single")

  override def peek(): Record = throw new NotImplementMethodException("peek")

  override def stream(): javaUtil.stream.Stream[Record] = {
    val spliterator = javaUtil.Spliterators.spliteratorUnknownSize(this, javaUtil.Spliterator.IMMUTABLE | javaUtil.Spliterator.ORDERED)
    javaUtil.stream.StreamSupport.stream(spliterator, false)
  }

  override def list(): javaUtil.List[Record] = throw new NotImplementMethodException("list()")

  override def consume(): ResultSummary = new MySummary(cypher, params)

  override def list[T](function: util.Function[Record, T]): javaUtil.List[T] = throw new NotImplementMethodException("list[T]")

  override def summary(): ResultSummary = new MySummary(cypher, params)


  class MySummary(cypher: String, params:Map[String,Any]) extends ResultSummary{
    override def statement(): Statement = {
      new Statement(cypher, mapAsJavaMap(params.asInstanceOf[Map[String, AnyRef]]))

    }

    override def counters(): SummaryCounters = {
      null
    }

    override def statementType(): StatementType = {
      StatementType.READ_WRITE
    }

    override def hasPlan: Boolean = false

    override def hasProfile: Boolean = false

    override def plan(): Plan = {
      println("plan")
      null
    }

    override def profile(): ProfiledPlan = {
      println("profile")
      null
    }

    override def notifications(): javaUtil.List[Notification] = {
      println("notifications")
      null
    }

    override def resultAvailableAfter(timeUnit: TimeUnit): Long = {
      receiveDataHeadTime
    }

    override def resultConsumedAfter(timeUnit: TimeUnit): Long = {
     resultConsumedAfterTime
    }

    override def server(): ServerInfo = {
      new ServerInfo {
        override def address(): String = uriAddress

        override def version(): String = "PandaDB-v0.3"
      }
    }
  }
}

