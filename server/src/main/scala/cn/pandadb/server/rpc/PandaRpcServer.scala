package cn.pandadb.server.rpc

import java.nio.ByteBuffer

import cn.pandadb.dbms.GraphDatabaseManager
import cn.pandadb.hipporpc.message.{CypherRequest, PeekOneDataRequest, PeekOneDataResponse, SayHelloRequest, SayHelloResponse, VerifyConnectionRequest, VerifyConnectionResponse}
import cn.pandadb.hipporpc.utils.{DriverValue, ValueConverter}
import cn.pandadb.hipporpc.values.Value
import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.modules.LifecycleServerModule
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
import org.opencypher.okapi.api.value.CypherValue

import scala.collection.mutable

class PandaRpcServer(config: Config, dbManager: GraphDatabaseManager)
  extends LifecycleServerModule with Logging{
  var rpcConfig:RpcEnvServerConfig = _
  var rpcEnv:HippoRpcEnv = _

  override def init(): Unit = {
    logger.info(this.getClass + ": init")

    rpcConfig = RpcEnvServerConfig(new RpcConf(), config.getRpcServerName(),
      config.getListenHost(), config.getRpcPort())
    rpcEnv = HippoRpcEnvFactory.create(rpcConfig)
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")

    val graphService = dbManager.getDatabase("default")
    val endpoint = new MyEndpoint(rpcEnv)
    val handler = new MyStreamHandler(graphService)
    rpcEnv.setupEndpoint(config.getRpcServerName(), endpoint)
    rpcEnv.setRpcHandler(handler)
    logger.info("default database: " + graphService.toString)
    rpcEnv.awaitTermination()
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
    rpcEnv.shutdown()
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }
}

class MyEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("server started...")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => context.reply(SayHelloResponse(s"$msg response"))
  }
}

class MyStreamHandler(graphFacade:GraphService) extends HippoRpcHandler {
  val converter = new ValueConverter
  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) =>
      context.reply(SayHelloResponse(msg.toUpperCase()))

    case PeekOneDataRequest(cypher, params) =>{
      val result = graphFacade.cypher(cypher, params).records
      val iter = result.iterator
      val metadata = result.physicalColumns.toList
      val record: DriverValue = {
        if (iter.hasNext){
          valueConverter(metadata, iter.next())
        }else{
          DriverValue(List(), Map())
        }
      }
      context.reply(PeekOneDataResponse(record))
    }

    case VerifyConnectionRequest(username, password) => {
      if (username == "panda" && password == "db"){
        context.reply(VerifyConnectionResponse("ok"))
      }else{
        context.reply(VerifyConnectionResponse("no"))
      }
    }
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case CypherRequest(cypher, params) =>{
      val result = graphFacade.cypher(cypher, params).records
      val metadata = result.physicalColumns.toList
      val pandaIterator = new PandaRecordsIterator(metadata, result.iterator)
      ChunkedStream.grouped(100, pandaIterator.toIterable)
    }
  }

  class PandaRecordsIterator(metadata: List[String], openCypherIter: Iterator[CypherValue.CypherMap]) extends Iterator[DriverValue]{
    override def hasNext: Boolean = openCypherIter.hasNext
    override def next(): DriverValue = {
      val cypherMap = openCypherIter.next()
      valueConverter(metadata, cypherMap)
    }
  }
  def valueConverter(metadata: List[String], cypherMap:CypherValue.CypherMap): DriverValue ={
    val rowMap = mutable.Map[String, Value]()
    val keys = cypherMap.keys
    keys.foreach(key => {
      val v = converter.converterValue(cypherMap.getOrElse(key))
      rowMap.put(key, v)
    })
    DriverValue(metadata, rowMap.toMap)
  }
}