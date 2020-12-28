import cn.pandadb.hipporpc.values.Value
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object client {
  def main(args: Array[String]): Unit = {
    val config =RpcEnvClientConfig(new RpcConf(), "client")
    val rpcEnv = HippoRpcEnvFactory.create(config)
    val endpointRef = rpcEnv.setupEndpointRef(new RpcAddress("localhost", 8878), "server")
//    val res = sayHelloHippoRpcTest(endpointRef, rpcEnv)
    val res = sendCypherRequest(endpointRef, rpcEnv)

    println(res)

    System.exit(0)
  }

  def sayHelloHippoRpcTest(endpointRef:HippoEndpointRef, rpcEnv: HippoRpcEnv): Any ={
    val res = Await.result(endpointRef.askWithBuffer[SayHelloResponse](SayHelloRequest("hello")),Duration.Inf)
    res.value
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
  def sendCypherRequest(endpointRef:HippoEndpointRef, rpcEnv: HippoRpcEnv): Unit ={
    val res = endpointRef.getChunkedStream[Value](CypherRequest("match (n) return n"), Duration.Inf)
    val iter = res.iterator
    while (iter.hasNext){
      println(iter.next())
    }
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
}


case class SayHelloRequest(msg: String)

case class SayHelloResponse(value: Any)

case class CypherRequest(cypher: String)