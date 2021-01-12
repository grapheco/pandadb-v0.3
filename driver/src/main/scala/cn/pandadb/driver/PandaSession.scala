package cn.pandadb.driver

import java.util

import org.neo4j.driver.{Bookmark, Query, Result, Session, Transaction, TransactionConfig, TransactionWork}
import org.neo4j.driver.internal.{AbstractQueryRunner, InternalResult}
import java.util.Collections.emptyMap
import java.util.Map

import org.neo4j.driver.async.ResultCursor
import org.neo4j.driver.internal.spi.Connection
import org.neo4j.driver.internal.util.Futures

import scala.collection.JavaConverters._

class PandaSession(rpcClient: PandaRpcClient) extends AbstractQueryRunner with Session {
//  override def run(statement: Statement, config: TransactionConfig): StatementResult = {
//    val cypher = statement.text()
//    val params = mapAsScalaMap(statement.parameters().asMap()).seq.toMap
//
//    val res = rpcClient.sendCypherRequest(cypher, params)
//    new PandaStatementResult(res, rpcClient, cypher, params)
//  }

  override def run(query: Query): Result = run(query, TransactionConfig.empty)

  override def run(query: String, config: TransactionConfig): Result = {
    run(query, emptyMap(), config)
  }

  override def run(query: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): Result = {
    run(new Query(query, parameters), config)
  }

  override def run(query: Query, config: TransactionConfig): Result = {
        val cypher = query.text()
        val params = mapAsScalaMap(query.parameters().asMap()).seq.toMap

        val res = rpcClient.sendCypherRequest(cypher, params)
        new PandaStatementResult(res, rpcClient, cypher, params)
  }

  override def beginTransaction(): Transaction = throw new NotImplementMethodException("beginTransaction")

  override def beginTransaction(transactionConfig: TransactionConfig): Transaction = throw new NotImplementMethodException("beginTransaction")

  override def readTransaction[T](transactionWork: TransactionWork[T]): T = throw new NotImplementMethodException("readTransaction")

  override def readTransaction[T](transactionWork: TransactionWork[T], transactionConfig: TransactionConfig): T = throw new NotImplementMethodException("readTransaction")

  override def writeTransaction[T](transactionWork: TransactionWork[T]): T = throw new NotImplementMethodException("writeTransaction")

  override def writeTransaction[T](transactionWork: TransactionWork[T], transactionConfig: TransactionConfig): T = throw new NotImplementMethodException("writeTransaction")

  override def lastBookmark(): Bookmark = throw new NotImplementMethodException("lastBookmark")

  override def reset(): Unit = {}

  override def close(): Unit = {}

  override def isOpen: Boolean = true
}
