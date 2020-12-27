package cn.pandadb.driver

import java.util

import cn.pandadb.driver.utils.TransferGrpcToScala
import org.neo4j.driver.types.TypeSystem
import org.neo4j.driver.{Record, Session, Statement, StatementResult, Transaction, TransactionConfig, TransactionWork, Value}

class PandaSession extends Session {
  val PANDA_CLIENT_NAME = "panda-client"
  val SERVER_NAME: String = "panda-server"
  val ADDRESS: String = "localhost"
  val PORT: Int = 50010
  val client = new PandaRpcClient(TransferGrpcToScala.getChannel(PORT))

  override def beginTransaction(): Transaction = ???

  override def beginTransaction(config: TransactionConfig): Transaction = ???

  override def readTransaction[T](work: TransactionWork[T]): T = ???

  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???

  override def writeTransaction[T](work: TransactionWork[T]): T = ???

  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???

  override def run(statementTemplate: String): StatementResult = {
    // grpc here
    val res = client.sendCypher()
    new PandaStatementResult(res)
//    ???
  }

  override def run(statement: String, config: TransactionConfig): StatementResult = ???

  override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = ???

  override def run(statement: Statement, config: TransactionConfig): StatementResult = ???

  override def lastBookmark(): String = ???

  override def reset(): Unit = ???

  override def close(): Unit = {
   client.close();
  }

  override def run(statementTemplate: String, parameters: Value): StatementResult = ???

  override def run(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): StatementResult = ???

  override def run(statementTemplate: String, statementParameters: Record): StatementResult = ???

  override def run(statement: Statement): StatementResult = ???

  override def typeSystem(): TypeSystem = ???

  override def isOpen: Boolean = ???

  case class CypherRequest(cypher: String)

}
