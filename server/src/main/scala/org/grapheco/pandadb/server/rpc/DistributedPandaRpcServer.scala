package org.grapheco.pandadb.server.rpc

import java.nio.ByteBuffer

import org.grapheco.pandadb.net.rpc.message.{CreateIndexRequest, CreateIndexResponse, CypherRequest, DropIndexRequest, DropIndexResponse, GetIndexedMetaRequest, GetIndexedMetaResponse, GetStatisticsRequest, GetStatisticsResponse, SayHelloRequest, SayHelloResponse}
import org.grapheco.pandadb.server.common.configuration.Config
import org.grapheco.pandadb.server.common.modules.LifecycleServerModule
import org.grapheco.pandadb.utils.ValueConverter
import com.typesafe.scalalogging.LazyLogging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
import org.grapheco.pandadb.dbms.DistributedGraphDatabaseManager
import org.grapheco.pandadb.net.rpc.utils.DriverValue
import org.grapheco.pandadb.net.rpc.values.Value

import scala.collection.mutable
import scala.concurrent.Await


class DistributedPandaRpcServer(config: Config, database: DistributedGraphDatabaseManager)
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
    val endpoint = new DistributedPandaEndpoint(rpcEnv)
    val handler = new DistributedPandaStreamHandler(database)
    rpcEnv.setupEndpoint(config.getRpcServerName(), endpoint)
    rpcEnv.setRpcHandler(handler)
    rpcEnv.awaitTermination()
  }

  override def stop(): Unit = {
    rpcEnv.shutdown()
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }
}

class DistributedPandaEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint with LazyLogging {

  override def onStart(): Unit = {
    logger.info("==== PandaDB Server started ====")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => context.reply(SayHelloResponse(s"$msg response"))
  }
}

class DistributedPandaStreamHandler(graphFacade: DistributedGraphDatabaseManager) extends HippoRpcHandler with LazyLogging {
  val converter = new ValueConverter

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) =>
      context.reply(SayHelloResponse(msg.toUpperCase()))

    case GetStatisticsRequest() => {
      val gf = graphFacade.defaultDB
      val statistics = gf.getStatistics
      val totalNodes = statistics.nodeCount
      val totalRels = statistics.relationCount
      val nodesByLabel = statistics.getNodeLabelCountMap.map(f => gf.getNodeLabelName(f._1).get -> f._2 )
      val relsByType = statistics.getRelationTypeCountMap.map(f => gf.getRelationTypeName(f._1).get -> f._2)
      val propsByIndex = statistics.getPropertyCountByIndex.map(f => gf.getPropertyName(f._1).get -> f._2)
      context.reply(GetStatisticsResponse(totalNodes, totalRels, nodesByLabel, relsByType, propsByIndex))
    }
    case GetIndexedMetaRequest() => {
      val indexStore = graphFacade.defaultDB.getIndexStore
      context.reply(GetIndexedMetaResponse(indexStore.getIndexMeta))
    }
    case CreateIndexRequest(label, props) => {
      val gf = graphFacade.defaultDB
      new Thread(){
        Thread.sleep(100)
        override def run(): Unit = gf.createIndexOnNode(label, props.toSet)
      }.start()

      context.reply(CreateIndexResponse(true))
    }

    case DropIndexRequest(label, propName) => {
      val gf = graphFacade.defaultDB
      new Thread(){
        Thread.sleep(100)
        override def run(): Unit = gf.dropIndexOnNode(label, propName)
      }.start()

      context.reply(DropIndexResponse(true))
    }
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case CypherRequest(cypher, params) => {
      try {
        val result = graphFacade.defaultDB.cypher(cypher, params)
        val metadata = result.columns().toList
        val data = result.records()
        val pandaIterator = new PandaRecordsIterator(metadata, data)
        ChunkedStream.grouped(100, pandaIterator.toIterable)
      } catch {
        case e: Exception => {
          logger.error(e.getMessage)
          ChunkedStream.grouped(1, new ExceptionMessage(e.getMessage).toIterable)
        }
      }
    }
  }

  class PandaRecordsIterator(metadata: List[String], lynxDataIterator: Iterator[Map[String, Any]]) extends Iterator[DriverValue] {
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
        _count += 1
        true
      } else false
    }

    override def next(): String = {
      message
    }
  }

}