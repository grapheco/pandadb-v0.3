package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.{GraphFacadeWithPPD, NodeIndex, NodeLabelIndex, NodeLabelStore, NodeStore, PropertyNameStore, RelationInEdgeIndexStore, RelationLabelIndex, RelationLabelStore, RelationOutEdgeIndexStore, RelationStore, RocksDBGraphAPI, RocksDBStorage, TokenStore}
import cn.pandadb.kernel.store.FileBasedIdGen
import org.apache.commons.io.FileUtils
import org.junit.{Before, Test}

import scala.util.Random

class GraphFacadePerformanceTest {
  val dbPath = "./testdata/output"

  var nodeLabelStore: TokenStore = _
  var relLabelStore: TokenStore = _
  var propNameStore: TokenStore = _
  var graphStore: RocksDBGraphAPI = _

  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()

    graphStore = new RocksDBGraphAPI("./testdata/output/rocksdb")

    nodeLabelStore = new NodeLabelStore(graphStore.getRocksDB)
    relLabelStore = new RelationLabelStore(graphStore.getRocksDB)
    propNameStore = new PropertyNameStore(graphStore.getRocksDB)

    graphFacade = new GraphFacadeWithPPD( nodeLabelStore, relLabelStore, propNameStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      graphStore,
      {}
    )

    graphFacade.addNode2(Map("id_p" -> 1L, "idStr" -> "a", "flag"->false), "person")
    graphFacade.addNode2(Map("id_p" -> 2L, "idStr" -> "b", "flag"->true), "worker")
    graphFacade.addNode2(Map("id_p" -> 3L, "idStr" -> "c", "flag"->false), "person")
    graphFacade.addNode2(Map("id_p" -> 4L, "idStr" -> "d", "flag"->true), "worker")
    graphFacade.addNode2(Map("id_p" -> 5L, "idStr" -> "e", "flag"->false), "person")
    graphFacade.addNode2(Map("id_p" -> 6L, "idStr" -> "f", "flag"->true), "person")

//    graphFacade.addRelation("Relation", 1, 2, Map())
//    graphFacade.addRelation("Relation", 3, 4, Map())
//    graphFacade.addRelation("Relation", 5, 6, Map())
  }
  
  @Test
  def basic1(): Unit ={
    var res = graphFacade.cypher("match (n) return n")
    res.show
    res = graphFacade.cypher("match ()-[r]->() return r")
    res.show
  }

  @Test
  def basic2(): Unit ={
    // use id(n) error
//    var res = graphFacade.cypher("match (n) where id(n)=1 return n")
//    res.show
//    var res = graphFacade.cypher("match (n) where n.id_p=1 return n")
//    res.show
//    res = graphFacade.cypher("match (n) where n.idStr='a' return n")
//    res.show
//    res = graphFacade.cypher("match (n) where n.flag=false return n")
//    res.show
  }
  @Test
  def basic3(): Unit ={
    var res = graphFacade.cypher("match (n) where n.id_p=1 and n.idStr='a' return n")
    res.show
    res = graphFacade.cypher("match (n) where n.id_p=1 and n.flag= false return n")
    res.show
    res = graphFacade.cypher("match (n) where n.idStr='c' and n.flag= false return n")
    res.show
  }
  @Test
  def basic4(): Unit ={
    // label with property error
    var res = graphFacade.cypher("match (n:person) return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.id_p=1 return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.idStr='a' return n")
    res.show
  }
}
