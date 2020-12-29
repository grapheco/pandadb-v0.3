import java.io.{File, FileInputStream}
import java.nio.ByteBuffer

import cn.pandadb.hipporpc.message.{Messages, SayHelloRequest, SayHelloResponse}
import cn.pandadb.hipporpc.utils.{DriverValue, ValueConverter}
import cn.pandadb.hipporpc.values.Value
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnvServerConfig}
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.grapheco.hippo.{ChunkedStream, HippoRpcHandler, ReceiveContext}

import scala.collection.mutable

object server {
  val PANDA_SERVER_NAME = "panda-server"

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var graphFacade: GraphFacadeWithPPD = _

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()

    val dbPath = "./testdata"
    var nodeStore = new NodeStoreAPI(dbPath)
    var relationStore = new RelationStoreAPI(dbPath)
    var indexStore = new IndexStoreAPI(dbPath)

    graphFacade = new GraphFacadeWithPPD(
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      nodeStore,
      relationStore,
      indexStore,
      {}
    )

    val n1: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode2(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode2(Map("name" -> "simba", "age" -> 10), "worker")
    val n4: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 50), "person")
    graphFacade.addRelation("friend", 1L, 2L, Map())

    //    val config = RpcEnvServerConfig(new RpcConf(), "server", args(0), 8878)
    val config = RpcEnvServerConfig(new RpcConf(), PANDA_SERVER_NAME, "localhost", 8878)
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
  }
  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case Messages(cypher) =>{
      // TODO: create a iterator,get batch data from resIterator
      val resIterator = graphFacade.cypher(cypher).records.iterator
      val toDriverRecordsList = mutable.ArrayBuffer[DriverValue]()
      while (resIterator.hasNext){
        val rowMap = mutable.Map[String, Value]()
        val cypherMap = resIterator.next()
        val keys = cypherMap.keys
        keys.foreach(key => {
          val v =  converter.converterValue(cypherMap.getOrElse(key))
          rowMap.put(key, v)
        })
        toDriverRecordsList += DriverValue(rowMap.toMap)
      }
      ChunkedStream.grouped(100, toDriverRecordsList)
    }
  }

}