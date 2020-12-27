package cn.pandadb.driver


import java.{util => javaUtil}

import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Record, StatementResult}

import scala.collection.JavaConverters._
class PandaStatementResult(name:String) extends StatementResult{

  override def keys(): javaUtil.List[String] = seqAsJavaList(List(name))

  override def hasNext: Boolean = ???

  override def next(): Record = ???

  override def single(): Record = ???

  override def peek(): Record = ???

  override def stream(): javaUtil.stream.Stream[Record] = {
    val spliterator = javaUtil.Spliterators.spliteratorUnknownSize(this, javaUtil.Spliterator.IMMUTABLE | javaUtil.Spliterator.ORDERED)
    javaUtil.stream.StreamSupport.stream(spliterator, false)
  }

  override def list(): javaUtil.List[Record] = ???

  override def list[T](mapFunction: javaUtil.function.Function[Record, T]): javaUtil.List[T] = ???

  override def consume(): ResultSummary = ???

  override def summary(): ResultSummary = ???
}
