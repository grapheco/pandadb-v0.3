package cn.pandadb.driver.rpc

import cn.pandadb.{CypherErrorException, VerifyConnectionMode}
import cn.pandadb.hipporpc.message._
import cn.pandadb.hipporpc.utils.DriverValue
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PandaRpcClient(hostName:String, port: Int, clientName: String, serverName: String) {
  val sparkConf = new RpcConf()
  sparkConf.set("spark.network.timeout", "3700s")
  val config: RpcEnvClientConfig = new RpcEnvClientConfig(sparkConf, clientName)
  val rpcEnv = HippoRpcEnvFactory.create(config)
  var endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)

  val DURATION_TIME = "3600s"

  def getStatistics(): GetStatisticsResponse ={
    Await.result(endpointRef.askWithBuffer[GetStatisticsResponse](GetStatisticsRequest()), Duration(DURATION_TIME))
  }
  def getIndexedMetaData(): GetIndexedMetaResponse = {
    Await.result(endpointRef.askWithBuffer[GetIndexedMetaResponse](GetIndexedMetaRequest()), Duration(DURATION_TIME))
  }
  def dropIndexMetaData(label: String, propName: String): DropIndexMetaResponse = {
    Await.result(endpointRef.askWithBuffer[DropIndexMetaResponse](DropIndexMetaRequest(label, propName)), Duration(DURATION_TIME))
  }

  def sendCypherRequest(cypher: String, params:Map[String, Any]): Stream[DriverValue] ={
    val res = endpointRef.getChunkedStream[Any](CypherRequest(cypher, params), Duration(DURATION_TIME))
    res.head match {
      case n: DriverValue => {
        res.asInstanceOf[Stream[DriverValue]]
      }
      case e: String => {
        throw new CypherErrorException(e)
      }
    }
  }

  def sendTransactionCypherRequest(uuid: String, cypher: String, params:Map[String, Any]): Stream[DriverValue] ={
    val res = endpointRef.getChunkedStream[Any](TransactionCypherRequest(uuid, cypher, params), Duration(DURATION_TIME))
    res.head match {
      case n: DriverValue => {
        res.asInstanceOf[Stream[DriverValue]]
      }
      case e: String => {
        throw new CypherErrorException(e)
      }
    }
  }
  def sendTransactionCommitRequest(uuid: String): String ={
    Await.result(endpointRef.askWithBuffer[TransactionCommitResponse](TransactionCommitRequest(uuid)), Duration(DURATION_TIME)).msg
  }
  def sendTransactionRollbackRequest(uuid: String): String ={
    Await.result(endpointRef.askWithBuffer[TransactionRollbackResponse](TransactionRollbackRequest(uuid)), Duration(DURATION_TIME)).msg
  }

  def verifyConnectionRequest(username:String, password: String): VerifyConnectionMode.Value = {
    Await.result(endpointRef.askWithBuffer[VerifyConnectionResponse](VerifyConnectionRequest(username, password)), Duration(DURATION_TIME)).result
  }

  def resetAccountRequest(username:String, password: String): VerifyConnectionMode.Value = {
    Await.result(endpointRef.askWithBuffer[ResetAccountResponse](ResetAccountRequest(username, password)), Duration(DURATION_TIME)).msg
  }

  def getPublicKey(): String = {
    Await.result(endpointRef.askWithBuffer[String](SecurityRequest()), Duration(DURATION_TIME))
  }

  def closeEndpointRef(): Unit ={
    rpcEnv.stop(endpointRef)
  }
  def createEndpointRef(): Unit ={
    endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)
  }

  def shutdown(): Unit ={
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
}