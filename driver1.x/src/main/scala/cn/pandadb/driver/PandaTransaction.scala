package cn.pandadb.driver

import java.util
import java.util.UUID
import java.util.concurrent.CompletionStage

import cn.pandadb.driver.rpc.PandaRpcClient
import com.typesafe.scalalogging.LazyLogging
import org.neo4j.driver.v1.types.TypeSystem
import org.neo4j.driver.v1.{Record, Statement, StatementResult, StatementResultCursor, Transaction, Value}

import scala.collection.JavaConverters._

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-25 09:14
 */
class PandaTransaction(rpcClient: PandaRpcClient, address: String) extends Transaction with LazyLogging{
  val uuid = UUID.randomUUID().toString

  override def success(): Unit = {
    val msg = rpcClient.sendTransactionCommitRequest(uuid)
    logger.debug(msg)
  }

  override def failure(): Unit = {
    val msg = rpcClient.sendTransactionRollbackRequest(uuid)
    logger.debug(msg)
  }

  override def close(): Unit = ???

  override def commitAsync(): CompletionStage[Void] = ???

  override def rollbackAsync(): CompletionStage[Void] = ???

  override def isOpen: Boolean = ???

  override def run(statementTemplate: String, parameters: Value): StatementResult = {
    run(statementTemplate, parameters.asMap())
  }

  override def runAsync(statementTemplate: String, parameters: Value): CompletionStage[StatementResultCursor] = ???

  override def run(statement: String, parameters: util.Map[String, AnyRef]): StatementResult = {
    val startTime = System.currentTimeMillis()
    val res = rpcClient.sendTransactionCypherRequest(uuid, statement, parameters.asScala.toMap)
    val receiveDataHeadTime = System.currentTimeMillis() - startTime
    new PandaStatementResult(res, statement, parameters.asScala.toMap, receiveDataHeadTime, address)
  }

  override def runAsync(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): CompletionStage[StatementResultCursor] = ???

  override def run(statementTemplate: String, statementParameters: Record): StatementResult = ???

  override def runAsync(statementTemplate: String, statementParameters: Record): CompletionStage[StatementResultCursor] = ???

  override def run(statementTemplate: String): StatementResult = run(statementTemplate, Map[String, AnyRef]().asJava)


  override def runAsync(statementTemplate: String): CompletionStage[StatementResultCursor] = ???

  override def run(statement: Statement): StatementResult = ???

  override def runAsync(statement: Statement): CompletionStage[StatementResultCursor] = ???

  override def typeSystem(): TypeSystem = ???
}
