package cn.pandadb.hipporpc

import cn.pandadb.{CypherErrorException, VerifyConnectionMode}
import cn.pandadb.hipporpc.message.{CypherRequest, ResetAccountRequest, ResetAccountResponse, SecurityRequest, VerifyConnectionRequest, VerifyConnectionResponse}
import cn.pandadb.hipporpc.utils.DriverValue
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PandaRpcClient(hostName:String, port: Int, clientName: String, serverName: String) {
  var config: RpcEnvClientConfig = new RpcEnvClientConfig(new RpcConf(), clientName)
  val rpcEnv = HippoRpcEnvFactory.create(config)

  var endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)

  def sendCypherRequest(cypher: String, params:Map[String, Any]): Stream[DriverValue] ={
    val res = endpointRef.getChunkedStream[Any](CypherRequest(cypher, params), Duration("30s"))
    res.head match {
      case n: DriverValue => {
        res.asInstanceOf[Stream[DriverValue]]
      }
      case e: String => {
        shutdown()
        throw new CypherErrorException(e)
      }
    }
  }

  def verifyConnectionRequest(username:String, password: String): VerifyConnectionMode.Value = {
    Await.result(endpointRef.askWithBuffer[VerifyConnectionResponse](VerifyConnectionRequest(username, password)), Duration("30s")).result
  }

  def resetAccountRequest(username:String, password: String): VerifyConnectionMode.Value = {
    Await.result(endpointRef.askWithBuffer[ResetAccountResponse](ResetAccountRequest(username, password)), Duration("30s")).msg
  }

  def getPublicKey(): String = {
    Await.result(endpointRef.askWithBuffer[String](SecurityRequest()), Duration("30s"))
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