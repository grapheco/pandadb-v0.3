package cn.pandadb.driver

import java.util
import java.util.concurrent.CompletionStage

import cn.pandadb.hipporpc.PandaRpcClient
import org.neo4j.driver.internal.{BoltServerAddress, Bookmarks, BookmarksHolder}
import org.neo4j.driver.v1.types.TypeSystem
import org.neo4j.driver.v1.{Record, Session, Statement, StatementResult, StatementResultCursor, StatementRunner, Transaction, TransactionConfig, TransactionWork, Value}
import java.util.Collections.emptyMap

import cn.pandadb.NotImplementMethodException

import scala.collection.JavaConverters._

class PandaSession(rpcClient: PandaRpcClient, address: String) extends StatementRunner with Session with BookmarksHolder {
  var isSessionClosed = false
  rpcClient.createEndpointRef()

  override def run(s: String, value: Value): StatementResult = run(s, value.asMap())

  override def runAsync(s: String, value: Value): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def run(s: String, map: util.Map[String, AnyRef]): StatementResult = {
    run(new Statement(s,map), TransactionConfig.empty())
  }

  override def runAsync(s: String, map: util.Map[String, AnyRef]): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def run(s: String, record: Record): StatementResult = throw new NotImplementMethodException("run")

  override def runAsync(s: String, record: Record): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def run(s: String): StatementResult = {
    synchronized{
      if (!isSessionClosed){
        val startTime = System.currentTimeMillis()
        val res = rpcClient.sendCypherRequest(s, Map())
        val receiveDataHeadTime = System.currentTimeMillis() - startTime
        new PandaStatementResult(res, s, Map(), receiveDataHeadTime, address)
      }
      else throw new SessionClosedException
    }
  }

  override def runAsync(s: String): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def run(statement: Statement): StatementResult = {
    run(statement, TransactionConfig.empty())
  }

  override def runAsync(statement: Statement): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def typeSystem(): TypeSystem = throw new NotImplementMethodException("typeSystem")

  override def beginTransaction(): Transaction = throw new NotImplementMethodException("beginTransactionAsync")

  override def beginTransaction(transactionConfig: TransactionConfig): Transaction = throw new NotImplementMethodException("beginTransactionAsync")

  override def beginTransaction(s: String): Transaction = throw new NotImplementMethodException("beginTransactionAsync")

  override def beginTransactionAsync(): CompletionStage[Transaction] = throw new NotImplementMethodException("beginTransactionAsync")

  override def beginTransactionAsync(transactionConfig: TransactionConfig): CompletionStage[Transaction] = throw new NotImplementMethodException("beginTransactionAsync")

  override def readTransaction[T](transactionWork: TransactionWork[T]): T = throw new NotImplementMethodException("readTransactionAsync")

  override def readTransaction[T](transactionWork: TransactionWork[T], transactionConfig: TransactionConfig): T = throw new NotImplementMethodException("readTransactionAsync")

  override def readTransactionAsync[T](transactionWork: TransactionWork[CompletionStage[T]]): CompletionStage[T] = throw new NotImplementMethodException("readTransactionAsync")

  override def readTransactionAsync[T](transactionWork: TransactionWork[CompletionStage[T]], transactionConfig: TransactionConfig): CompletionStage[T] = throw new NotImplementMethodException("readTransactionAsync")

  override def writeTransaction[T](transactionWork: TransactionWork[T]): T = throw new NotImplementMethodException("writeTransaction")

  override def writeTransaction[T](transactionWork: TransactionWork[T], transactionConfig: TransactionConfig): T = throw new NotImplementMethodException("writeTransaction")

  override def writeTransactionAsync[T](transactionWork: TransactionWork[CompletionStage[T]]): CompletionStage[T] = throw new NotImplementMethodException("writeTransactionAsync")

  override def writeTransactionAsync[T](transactionWork: TransactionWork[CompletionStage[T]], transactionConfig: TransactionConfig): CompletionStage[T] = throw new NotImplementMethodException("writeTransactionAsync")

  override def run(s: String, transactionConfig: TransactionConfig): StatementResult = {
    run(s, emptyMap(), transactionConfig)
  }

  override def run(s: String, map: util.Map[String, AnyRef], transactionConfig: TransactionConfig): StatementResult = {
    run(new Statement(s, map), transactionConfig)
  }

  override def run(statement: Statement, transactionConfig: TransactionConfig): StatementResult = {
    synchronized{
      if (!isSessionClosed){
        val cypher = statement.text()
        val params = mapAsScalaMapConverter(statement.parameters().asMap()).asScala.seq.toMap
        val startTime = System.currentTimeMillis()
        val res = rpcClient.sendCypherRequest(cypher, params)
        val receiveDataHeadTime = System.currentTimeMillis() - startTime
        new PandaStatementResult(res, cypher, params, receiveDataHeadTime, address)
      } else throw new SessionClosedException
    }
  }

  override def runAsync(s: String, transactionConfig: TransactionConfig): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def runAsync(s: String, map: util.Map[String, AnyRef], transactionConfig: TransactionConfig): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def runAsync(statement: Statement, transactionConfig: TransactionConfig): CompletionStage[StatementResultCursor] = throw new NotImplementMethodException("runAsync")

  override def lastBookmark(): String = throw new NotImplementMethodException("lastBookmark")

  override def reset(): Unit = {}

  override def close(): Unit = {
    isSessionClosed = true
    rpcClient.closeEndpointRef()
  }

  override def closeAsync(): CompletionStage[Void] = throw new NotImplementMethodException("closeAsync")

  override def getBookmarks: Bookmarks = throw new NotImplementMethodException("getBookmarks")

  override def setBookmarks(bookmarks: Bookmarks): Unit = throw new NotImplementMethodException("setBookmarks")

  // should dynamic
  override def isOpen: Boolean = true
}

class SessionClosedException extends Exception{
  override def getMessage: String = "session is closed"
}