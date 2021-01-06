import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.pandadb.hipporpc.message.{CypherRequest, PeekOneDataRequest, PeekOneDataResponse, SayHelloRequest, SayHelloResponse, VerifyConnectionRequest, VerifyConnectionResponse}
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
    val endpoint = new MyEndpoint(rpcEnv)
    val handler = new MyStreamHandler(graphFacade)
    rpcEnv.setupEndpoint(PANDA_SERVER_NAME, endpoint)
    rpcEnv.setRpcHandler(handler)
    rpcEnv.awaitTermination()
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

class MyStreamHandler(graphFacade:GraphFacadeWithPPD) extends HippoRpcHandler {
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
