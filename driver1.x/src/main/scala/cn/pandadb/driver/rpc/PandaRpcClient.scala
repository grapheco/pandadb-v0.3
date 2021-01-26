package cn.pandadb.driver.rpc

import cn.pandadb.{CypherErrorException, VerifyConnectionMode}
import cn.pandadb.hipporpc.message._
import cn.pandadb.hipporpc.utils.DriverValue
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.neo4j.driver.v1.exceptions.SessionExpiredException

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PandaRpcClient(hostName:String, port: Int, clientName: String, serverName: String) {
  var config: RpcEnvClientConfig = new RpcEnvClientConfig(new RpcConf(), clientName)
  val rpcEnv = HippoRpcEnvFactory.create(config)
  var endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)

  val DURATION_TIME = "21600s"

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