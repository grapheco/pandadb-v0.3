import cn.pandadb.hipporpc.message.{CypherRequest, SayHelloRequest, SayHelloResponse}
import cn.pandadb.hipporpc.utils.DriverValue
import cn.pandadb.hipporpc.values.Value
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.{HippoEndpointRef, HippoRpcEnv, HippoRpcEnvFactory}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object client {
  val PANDA_SERVER_NAME = "panda-server"

  def main(args: Array[String]): Unit = {
    val config =RpcEnvClientConfig(new RpcConf(), "client")
    val rpcEnv = HippoRpcEnvFactory.create(config)
    val endpointRef = rpcEnv.setupEndpointRef(new RpcAddress("localhost", 8878), PANDA_SERVER_NAME)
//    val res = sayHelloHippoRpcTest(endpointRef, rpcEnv)
    val res = sendCypherRequest(endpointRef, rpcEnv)

    println(res)

    System.exit(0)
  }

  def sayHelloHippoRpcTest(endpointRef:HippoEndpointRef, rpcEnv: HippoRpcEnv): Any ={
    val res = Await.result(endpointRef.askWithBuffer[SayHelloResponse](SayHelloRequest("hello")),Duration.Inf)
    println(res.msg)
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
  def sendCypherRequest(endpointRef:HippoEndpointRef, rpcEnv: HippoRpcEnv): Unit ={
    val res = endpointRef.getChunkedStream[Map[String, Value]](CypherRequest("match (n) return n, n.name"), Duration.Inf)
    val iter = res.iterator
    while (iter.hasNext){
      val rec = iter.next()
      println(rec)
      println("=========================================")
    }
    rpcEnv.stop(endpointRef)
    rpcEnv.shutdown()
  }
}