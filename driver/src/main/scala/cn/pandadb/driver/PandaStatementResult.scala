//package cn.pandadb.driver
//
//
//import java.{util => javaUtil}
//
//import cn.pandadb.grpc.InternalRecords
//import org.neo4j.driver.summary.ResultSummary
//import org.neo4j.driver.{Record, StatementResult}
//
//import scala.collection.JavaConverters._
//class PandaStatementResult(internalRecords: InternalRecords) extends StatementResult{
//  val totalRecordsNum = internalRecords.getRecordCount
//  var count = 0
//
//  override def keys(): javaUtil.List[String] = {
//    val record = internalRecords.getRecord(count)
//    seqAsJavaList(record.getRecordName)
//  }
//
//  override def hasNext: Boolean = {
//    if (count < totalRecordsNum){
//      true
//    }else false
//  }
//
//  override def next(): Record = {
//    val record = internalRecords.getRecord(count)
//    count += 1
//    new PandaRecord(record)
//  }
//
//  override def single(): Record = ???
//
//  override def peek(): Record = ???
//
//  override def stream(): javaUtil.stream.Stream[Record] = {
//    val spliterator = javaUtil.Spliterators.spliteratorUnknownSize(this, javaUtil.Spliterator.IMMUTABLE | javaUtil.Spliterator.ORDERED)
//    javaUtil.stream.StreamSupport.stream(spliterator, false)
//  }
//
//  override def list(): javaUtil.List[Record] = ???
//
//  override def list[T](mapFunction: javaUtil.function.Function[Record, T]): javaUtil.List[T] = ???
//
//  override def consume(): ResultSummary = ???
//
//  override def summary(): ResultSummary = ???
//}
