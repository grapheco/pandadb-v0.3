package cn.pandadb.driver

import cn.pandadb.hipporpc.message.{CypherRequest, PeekOneDataRequest, PeekOneDataResponse, VerifyConnectionRequest, VerifyConnectionResponse}
import cn.pandadb.hipporpc.utils.DriverValue
import cn.pandadb.hipporpc.values.{Value => HippoValue}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PandaRpcClient(hostName:String, port: Int, clientName: String, serverName: String) {
  var config: RpcEnvClientConfig = new RpcEnvClientConfig(new RpcConf(), clientName)
  val rpcEnv = HippoRpcEnvFactory.create(config)
  val endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)
  def sendCypherRequest(cypher: String): Iterator[DriverValue] ={
    endpointRef.getChunkedStream[DriverValue](CypherRequest(cypher), Duration.Inf).iterator

  }
  def peekOneDataRequest(cypher: String): DriverValue = {
    // maybe to async?
    Await.result(endpointRef.askWithBuffer[PeekOneDataResponse](PeekOneDataRequest(cypher)), Duration("30s")).driverValue
  }

  def verifyConnectionRequest(username:String, password: String): String = {
    Await.result(endpointRef.askWithBuffer[VerifyConnectionResponse](VerifyConnectionRequest(username, password)), Duration("30s")).result
  }

  def close: Unit ={
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
}