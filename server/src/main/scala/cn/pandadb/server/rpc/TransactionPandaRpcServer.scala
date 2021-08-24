package cn.pandadb.server.rpc

import java.nio.ByteBuffer

import cn.pandadb.VerifyConnectionMode
import cn.pandadb.dbms.{GraphDatabaseManager, RsaSecurity, TransactionGraphDatabaseManager}
import cn.pandadb.hipporpc.message._
import cn.pandadb.hipporpc.utils.DriverValue
import cn.pandadb.hipporpc.values.Value
import cn.pandadb.kernel.GraphService
import cn.pandadb.kernel.kv.meta.Auth
import cn.pandadb.kernel.transaction.PandaTransactionManager
import cn.pandadb.server.common.configuration.Config
import cn.pandadb.server.common.modules.LifecycleServerModule
import cn.pandadb.utils.ValueConverter
import com.typesafe.scalalogging.LazyLogging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}

import scala.collection.mutable


class TransactionPandaRpcServer(config: Config, dbManager: TransactionGraphDatabaseManager)
  extends LifecycleServerModule with LazyLogging {
  var rpcConfig: RpcEnvServerConfig = _
  var rpcEnv: HippoRpcEnv = _

  override def init(): Unit = {
    logger.debug(this.getClass + ": init")
    rpcConfig = RpcEnvServerConfig(new RpcConf(), config.getRpcServerName(), config.getListenHost(), config.getRpcPort())
    rpcEnv = HippoRpcEnvFactory.create(rpcConfig)
  }

  override def start(): Unit = {
    logger.debug(this.getClass + ": start")

    val graphService = dbManager.getDatabase()

    val endpoint = new TransactionPandaEndpoint(rpcEnv)
    val handler = new TransactionPandaStreamHandler(graphService)
    rpcEnv.setupEndpoint(config.getRpcServerName(), endpoint)
    rpcEnv.setRpcHandler(handler)
    logger.debug("database: " + graphService.toString)
    rpcEnv.awaitTermination()
  }

  override def stop(): Unit = {
    rpcEnv.shutdown()
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }
}

class TransactionPandaEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint with LazyLogging {

  override def onStart(): Unit = {
    logger.info("==== PandaDB Server started ====")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => context.reply(SayHelloResponse(s"$msg response"))
  }
}

class TransactionPandaStreamHandler(graphFacade: PandaTransactionManager) extends HippoRpcHandler {
  val converter = new ValueConverter

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) =>
      context.reply(SayHelloResponse(msg.toUpperCase()))
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case CypherRequest(cypher, params) => {
      val tx = graphFacade.begin()
      try {
        val result = tx.execute(cypher, params)
        val metadata = result.columns().toList
        val data = result.records()
        val pandaIterator = new TransactionPandaRecordsIterator(metadata, data)
        ChunkedStream.grouped(100, pandaIterator.toIterable)
      } catch {
        case e: Exception => ChunkedStream.grouped(1, new ExceptionMessage(e.getMessage).toIterable)
      }finally {
        tx.commit()
      }
    }
  }

  class TransactionPandaRecordsIterator(metadata: List[String], lynxDataIterator: Iterator[Map[String, Any]]) extends Iterator[DriverValue] {
    var isPutMetadata = false
    var isUsed = false

    override def hasNext: Boolean = {
      if (!isPutMetadata) {
        isPutMetadata = true
        true
      } else {
        lynxDataIterator.hasNext
      }
    }

    override def next(): DriverValue = {
      if (!isUsed && isPutMetadata) {
        isUsed = true
        val metaMap = mutable.Map[String, Value]()
        metadata.foreach(f => metaMap.put(f, null))
        DriverValue(metaMap.toMap)
      } else {
        val lynxValue = lynxDataIterator.next()
        valueConverter(lynxValue)
      }
    }
  }

  def valueConverter(lynxValue: Map[String, Any]): DriverValue = {
    val rowMap = mutable.Map[String, Value]()
    val keys = lynxValue.keys
    keys.foreach(key => {
      val v = converter.converterValue(lynxValue(key))
      rowMap.put(key, v)
    })
    DriverValue(rowMap.toMap)
  }

  class ExceptionMessage(message: String) extends Iterator[String] {
    var _count = 1

    override def hasNext: Boolean = {
      if (_count == 1) {
        true
      } else false
    }

    override def next(): String = {
      message
    }
  }

}