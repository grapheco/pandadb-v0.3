package cn.pandadb.driver

import java.util

import org.neo4j.driver.internal.AbstractStatementRunner
import org.neo4j.driver.types.TypeSystem
import org.neo4j.driver.{Record, Session, Statement, StatementResult, Transaction, TransactionConfig, TransactionWork, Value}

import scala.collection.JavaConverters._

class PandaSession(rpcClient: PandaRpcClient) extends AbstractStatementRunner with Session {
  override def beginTransaction(): Transaction = ???

  override def beginTransaction(config: TransactionConfig): Transaction = ???

  override def readTransaction[T](work: TransactionWork[T]): T = ???

  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???

  override def writeTransaction[T](work: TransactionWork[T]): T = ???

  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???

  override def run(statement: String, config: TransactionConfig): StatementResult = ???

  override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = ???

  override def run(statement: Statement, config: TransactionConfig): StatementResult = ???

  override def lastBookmark(): String = ???

  override def reset(): Unit = ???

  override def close(): Unit = {}

  override def run(statement: Statement): StatementResult = {
    val cypher = statement.text()
    val params = mapAsScalaMap(statement.parameters().asMap()).seq.toMap

    val res = rpcClient.sendCypherRequest(cypher, params)
    new PandaStatementResult(res, rpcClient, cypher, params)
  }

  override def isOpen: Boolean = ???

}
