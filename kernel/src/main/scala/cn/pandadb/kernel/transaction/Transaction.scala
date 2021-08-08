package cn.pandadb.kernel.transaction

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:45 上午 2021/8/6
 * @Modified By:
 */


// todo
class Transaction(id: String) {

  val queryStates: ArrayBuffer[QueryStat] = new ArrayBuffer[QueryStat]()

  def execute(cypherStat: String): Unit = {

    val queryStats = QueryStat(cypherStat, QUERYSTATUS.EXECUTING)
    queryStates.append(queryStats)
    try {
      //execute the query
      queryStats.status = QUERYSTATUS.SUCCEED
    } catch {
      case e : Exception => queryStats.status = QUERYSTATUS.FAILED
    }
  }

  def commit(): Unit = {
    // check all the status
  }


  def rollback(): Unit = {
    // hard to implement
  }

  def toLogString: String = {
    ""
  }

}