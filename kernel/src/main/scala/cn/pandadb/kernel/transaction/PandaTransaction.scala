package cn.pandadb.kernel.transaction

import cn.pandadb.kernel.kv.TransactionGraphFacade
import cn.pandadb.kernel.util.CommonUtils
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.{LynxResult, LynxTransaction}
import org.rocksdb.Transaction

import scala.collection.mutable.ArrayBuffer

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:45 上午 2021/8/6
 * @Modified By:
 */

class PandaTransaction(val id: String, val rocksTxMap: Map[String, Transaction],
                       private val graphFacade: TransactionGraphFacade,
                       txWatcher: TransactionWatcher)
  extends LynxTransaction with LazyLogging{

  val queryStates: ArrayBuffer[QueryStat] = new ArrayBuffer[QueryStat]()
  var isContainWriteCypher = new ArrayBuffer[Boolean]()

  def execute(cypherStat: String, parameters: Map[String, Any]): LynxResult = {

    val queryStats = QueryStat(cypherStat, QUERYSTATUS.EXECUTING)
    queryStates.append(queryStats)

    try {
      //execute the query
      val isWriteCypher = CommonUtils.isWriteCypher(cypherStat.toLowerCase())
      isContainWriteCypher.append(isWriteCypher)

      if (isWriteCypher) txWatcher.increase()

      val res = graphFacade.cypher(cypherStat, parameters, Option(this))
      queryStats.status = QUERYSTATUS.SUCCEED
      res
    } catch {
      case e : Exception => {
        queryStats.status = QUERYSTATUS.FAILED
        rollback()
        throw new PandaDBException(s"${e.getMessage}")
      }
    }
  }

  def commit(): Unit = {
    if (isContainWriteCypher.contains(true)){
      graphFacade.refresh(Option(this))
      val logWriter = graphFacade.getLogWriter()
      val writeTxId = logWriter.flushUndoLog()

      rocksTxMap.foreach(f => {
        f._2.commit()
      })

      logWriter.writeGuardLog(writeTxId)
      logWriter.flushGuardLog()
      txWatcher.decrease()
    }
  }


  def rollback(): Unit = {
    logger.info("......roll back transaction......")
    rocksTxMap.values.foreach(_.rollback())
  }

  def toLogString: String = {
    ""
  }

}