package cn.pandadb.drivertest

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.pandadb.dbms.RsaSecurity
import cn.pandadb.hipporpc.message.{CypherRequest, SayHelloRequest, SayHelloResponse, SecurityRequest, VerifyConnectionRequest, VerifyConnectionResponse}
import cn.pandadb.hipporpc.utils.{DriverValue, ValueConverter}
import cn.pandadb.hipporpc.values.Value
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.ir.impl.exception.ParsingException
import org.opencypher.v9_0.util.SyntaxException

import scala.collection.mutable

object server {
  val PANDA_SERVER_NAME = "pandadb-server"

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File("./testdata"))
    new File("./testdata/output").mkdirs()

    val dbPath = "./testdata"
    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacadeWithPPD(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )

    val n1: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode2(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode2(Map("name" -> "simba", "age" -> 10), "worker")
    val n4: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 50), "person")
    graphFacade.addRelation("friend", 1L, 2L, Map())

    //    val config = RpcEnvServerConfig(new RpcConf(), "server", args(0), 8878)
    val config = RpcEnvServerConfig(new RpcConf(), PANDA_SERVER_NAME, "0.0.0.0", 8878)
    val rpcEnv = HippoRpcEnvFactory.create(config)
    val endpoint = new PandaRpcEndpoint(rpcEnv)
    val handler = new PandaStreamHandler(graphFacade)
    rpcEnv.setupEndpoint(PANDA_SERVER_NAME, endpoint)
    rpcEnv.setRpcHandler(handler)
    rpcEnv.awaitTermination()
  }
}
class PandaRpcEndpoint(override val rpcEnv: HippoRpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("server started...")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHelloRequest(msg) => context.reply(SayHelloResponse(s"$msg response"))
  }
}

class PandaStreamHandler(graphFacade:GraphFacadeWithPPD) extends HippoRpcHandler {
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