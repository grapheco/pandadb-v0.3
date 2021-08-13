package cn.pandadb.kernel.transaction

import cn.pandadb.kernel.kv.TransactionGraphFacade
import org.grapheco.lynx.{LynxResult, LynxTransaction}
import org.rocksdb.Transaction

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:45 上午 2021/8/6
 * @Modified By:
 */

class PandaTransaction(private val id: String, val rocksTxMap: Map[String, Transaction], private val graphFacade: TransactionGraphFacade) extends LynxTransaction{

  val queryStates: ArrayBuffer[QueryStat] = new ArrayBuffer[QueryStat]()
  def execute(cypherStat: String): LynxResult = {

    val res = graphFacade.cypher(cypherStat, Map.empty, Option(this))

    val queryStats = QueryStat(cypherStat, QUERYSTATUS.EXECUTING)
    queryStates.append(queryStats)
    try {
      //execute the query
      queryStats.status = QUERYSTATUS.SUCCEED
    } catch {
      case e : Exception => queryStats.status = QUERYSTATUS.FAILED
    }

    res
  }

  def commit(): Unit = {
    // check all the status
    rocksTxMap.values.foreach(_.commit())
  }


  def rollback(): Unit = {
    rocksTxMap.values.foreach(_.rollback())
  }

  def toLogString: String = {
    ""
  }

}