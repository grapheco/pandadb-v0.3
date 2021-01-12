package cn.pandadb.server.rpc

import java.nio.ByteBuffer

import cn.pandadb.dbms.{GraphDatabaseManager, RsaSecurity}
import cn.pandadb.hipporpc.message.{CypherRequest, SayHelloRequest, SayHelloResponse, SecurityRequest, VerifyConnectionRequest, VerifyConnectionResponse}
import cn.pandadb.hipporpc.utils.DriverValue
import cn.pandadb.hipporpc.values.Value
import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.server.common.Logging
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.modules.LifecycleServerModule
import cn.pandadb.utils.ValueConverter
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.v9_0.util.SyntaxException

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
    val endpoint = new PandaEndpoint(rpcEnv)
    val handler = new PandaStreamHandler(graphService)
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

class PandaEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("server started...")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => context.reply(SayHelloResponse(s"$msg response"))
  }
}

class PandaStreamHandler(graphFacade:GraphService) extends HippoRpcHandler {
  val converter = new ValueConverter
  RsaSecurity.init()

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) =>
      context.reply(SayHelloResponse(msg.toUpperCase()))

    case VerifyConnectionRequest(usernameKey, passwordKey) => {
      val username = RsaSecurity.rsaDecrypt(usernameKey, RsaSecurity.getPrivateKeyStr())
      val password = RsaSecurity.rsaDecrypt(passwordKey, RsaSecurity.getPrivateKeyStr())

      if (username == "panda" && password == "db"){
        context.reply(VerifyConnectionResponse("ok"))
      }else{
        context.reply(VerifyConnectionResponse("no"))
      }
    }
    case SecurityRequest() => {
      context.reply(RsaSecurity.getPublicKeyStr())
    }
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case CypherRequest(cypher, params) => {
      try {
        val result = graphFacade.cypher(cypher, params).records
        val metadata = result.physicalColumns.toList
        val pandaIterator = new PandaRecordsIterator(metadata, result.iterator)
        ChunkedStream.grouped(100, pandaIterator.toIterable)
      }catch {
        case e:Exception => ChunkedStream.grouped(1, new ExceptionMessage(e.getMessage).toIterable)
      }
    }
  }

  class PandaRecordsIterator(metadata: List[String], openCypherIter: Iterator[CypherValue.CypherMap]) extends Iterator[DriverValue]{
    var isPutMetadata = false
    var isUsed = false

    override def hasNext: Boolean = {
      if (!isPutMetadata){
        isPutMetadata = true
        true
      }else{
        openCypherIter.hasNext
      }
    }
    override def next(): DriverValue = {
      if (!isUsed && isPutMetadata){
        isUsed = true
        val metaMap = mutable.Map[String, Value]()
        metadata.foreach(f => metaMap.put(f, null))
        DriverValue(metaMap.toMap)
      }else{
        val cypherMap = openCypherIter.next()
        valueConverter(metadata, cypherMap)
      }
    }
  }
  def valueConverter(metadata: List[String], cypherMap:CypherValue.CypherMap): DriverValue ={
    val rowMap = mutable.Map[String, Value]()
    val keys = cypherMap.keys
    keys.foreach(key => {
      val v = converter.converterValue(cypherMap.getOrElse(key))
      rowMap.put(key, v)
    })
    DriverValue(rowMap.toMap)
  }

  class ExceptionMessage(message: String) extends Iterator[String]{
    var _count = 1
    override def hasNext: Boolean = {
      if (_count == 1){
        true
      }else false
    }
    override def next(): String = {
      message
    }
  }
}