package cn.pandadb.hipporpc

import cn.pandadb.CypherErrorException
import cn.pandadb.hipporpc.message.{CypherRequest, SecurityRequest, VerifyConnectionRequest, VerifyConnectionResponse}
import cn.pandadb.hipporpc.utils.DriverValue
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PandaRpcClient(hostName:String, port: Int, clientName: String, serverName: String) {
  var config: RpcEnvClientConfig = new RpcEnvClientConfig(new RpcConf(), clientName)
  val rpcEnv = HippoRpcEnvFactory.create(config)
  val endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)

  def sendCypherRequest(cypher: String, params:Map[String, Any]): Stream[DriverValue] ={
    val res = endpointRef.getChunkedStream[Any](CypherRequest(cypher, params), Duration.Inf)
    res.head match {
      case n: DriverValue => {
        res.asInstanceOf[Stream[DriverValue]]
      }
      case e: String => {
        close
        throw new CypherErrorException(e)
      }
    }
  }

  def verifyConnectionRequest(username:String, password: String): String = {
    Await.result(endpointRef.askWithBuffer[VerifyConnectionResponse](VerifyConnectionRequest(username, password)), Duration("5s")).result
  }

  def getPublicKey(): String = {
    Await.result(endpointRef.askWithBuffer[String](SecurityRequest()), Duration("5s"))
  }

  def close: Unit ={
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
}