package cn.pandadb.driver

import cn.pandadb.hipporpc.message.Messages
import cn.pandadb.hipporpc.utils.DriverValue
import cn.pandadb.hipporpc.values.{Value => HippoValue}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}

import scala.concurrent.duration.Duration

class PandaRpcClient(hostName:String, port: Int, clientName: String, serverName: String) {
  var config: RpcEnvClientConfig = new RpcEnvClientConfig(new RpcConf(), clientName)
  val rpcEnv = HippoRpcEnvFactory.create(config)
  val endpointRef = rpcEnv.setupEndpointRef(new RpcAddress(hostName, port), serverName)

  def sendCypherRequest(cypher: String): Iterator[DriverValue] ={
    endpointRef.getChunkedStream[DriverValue](Messages(cypher), Duration.Inf).iterator
  }

  def close: Unit ={
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
}