package cn.pandadb.kernel.transaction

import cn.pandadb.kernel.kv.TransactionGraphFacade
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import org.grapheco.lynx.{LynxResult, LynxTransaction}
import org.rocksdb.Transaction

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:45 上午 2021/8/6
 * @Modified By:
 */

class PandaTransaction(val id: String, val rocksTxMap: Map[String, Transaction], private val graphFacade: TransactionGraphFacade) extends LynxTransaction{

  val queryStates: ArrayBuffer[QueryStat] = new ArrayBuffer[QueryStat]()
  def execute(cypherStat: String, parameters: Map[String, Any]): LynxResult = {

    val queryStats = QueryStat(cypherStat, QUERYSTATUS.EXECUTING)
    queryStates.append(queryStats)

    try {
      //execute the query
      val res = graphFacade.cypher(cypherStat, parameters, Option(this))
      queryStats.status = QUERYSTATUS.SUCCEED
      res
    } catch {
      case e : Exception => {
        queryStats.status = QUERYSTATUS.FAILED
        throw new PandaDBException(s"execute cypher failed...")
      }
    }
  }

  def commit(): Unit = {
    graphFacade.getLogWriter().flush()
    // check all the status
    rocksTxMap.foreach(f => {
      f._2.commit()
    })
  }


  def rollback(): Unit = {
    rocksTxMap.values.foreach(_.rollback())
  }

  def toLogString: String = {
    ""
  }

}