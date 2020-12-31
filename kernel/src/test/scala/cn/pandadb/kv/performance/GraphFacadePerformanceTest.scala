package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.kv.{GraphFacadeWithPPD}//, NodeIndex, NodeLabelIndex, NodeLabelStore, NodeStore, PropertyNameStore, RelationInEdgeIndexStore, RelationLabelIndex, RelationLabelStore, RelationOutEdgeIndexStore, RelationStore, RocksDBGraphAPI, RocksDBStorage, TokenStore}
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.junit.{Before, Test}

import scala.util.Random

class GraphFacadePerformanceTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata"))
    new File("./testdata/output").mkdirs()

    val dbPath = "./testdata"
    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath+"/statistics")

    //    graphStore = new RocksDBGraphAPI("./testdata/output/rocksdb")
    //    nodeLabelStore = new NodeLabelNameStore(graphStore.getRocksDB)
    //    relLabelStore = new RelationTypeNameStore(graphStore.getRocksDB)
    //    propNameStore = new PropertyNameStore(graphStore.getRocksDB)




    graphFacade = new GraphFacadeWithPPD(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )


    graphFacade.addNode2(Map("id_p" -> 1L, "idStr" -> "a", "flag"->false), "person")
    graphFacade.addNode2(Map("id_p" -> 2L, "idStr" -> "b", "flag"->true), "worker")
    graphFacade.addNode2(Map("id_p" -> 3L, "idStr" -> "c", "flag"->false), "person")
    graphFacade.addNode2(Map("id_p" -> 4L, "idStr" -> "d", "flag"->true), "worker")
    graphFacade.addNode2(Map("id_p" -> 5L, "idStr" -> "e", "flag"->false), "person")
    graphFacade.addNode2(Map("id_p" -> 6L, "idStr" -> "f", "flag"->true), "person")
    graphFacade.addNode2(Map("id_p" -> 1L, "idStr" -> "a", "flag"->true), "person")
    graphFacade.addNode2(Map("id_p" -> 1L, "idStr" -> "a", "flag"->false), "person")


    graphFacade.addRelation("Relation", 1, 2, Map())
    graphFacade.addRelation("Relation", 3, 4, Map())
    graphFacade.addRelation("Relation", 5, 6, Map())
  }

  @Test
  def testQueryAll(): Unit ={
    var res = graphFacade.cypher("match (n) return n limit 1")
    res.show
    return
    res = graphFacade.cypher("match ()-[r]->() return r")
    res.show
    res = graphFacade.cypher("match (n:person)-[r]->() return r")
    res.show
  }

  @Test
  def testFilterWithSingleProperty(): Unit ={
    var res = graphFacade.cypher("match (n) where n.id_p=1 return n")
    res.show
    res = graphFacade.cypher("match (n) where n.idStr='a' return n")
    res.show
    res = graphFacade.cypher("match (n) where n.flag=false return n")
    res.show
  }
  @Test
  def testFilterWithMultipleProperties(): Unit ={
    var res = graphFacade.cypher("match (n) where n.id_p=1 and n.idStr='a' return n")
    res.show
    res = graphFacade.cypher("match (n) where n.id_p=1 and n.flag= false return n")
    res.show
    res = graphFacade.cypher("match (n) where n.idStr='c' and n.flag= false return n")
    res.show
  }
  @Test
  def testFilterWithLabelAndProperties(): Unit ={
    var res = graphFacade.cypher("match (n:person) return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.id_p=1 return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.idStr='a' return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.flag = false return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.idStr = 'a' return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.flag = false return n")
    res.show
    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.idStr = 'a' and n.flag = false return n")
    res.show
  }

  @Test
  def testQueryfunctions(): Unit ={
    // functions error
//    var res = graphFacade.cypher("match (n) where id(n)=1 return n")
//    res.show
//    var res = graphFacade.cypher("match (n) return max(n.id_p)")
//    res.show
//    var res = graphFacade.cypher("match (n) return sum(n.id_p)")
//    res.show
  }

  def timing(cy: String): (String, Long) = {
    val t1 = System.currentTimeMillis()
    graphFacade.cypher(cy)
    val t2 = System.currentTimeMillis()
    cy->(t2-t1)
  }

  @Test
  def testTime(): Unit = {
    var cyphers1: Array[String] = Array("match (n:person) return n",
      "match (n) return n",
      "match (n:person) return n",
      "match (n:person) where n.name = 'joe' return n",
      "match (n:student) where n.age = 100 return n",
      "match (n:person) return n",
      "match (n:person) return n",
      "match (n:person) return n",
      "match (n:person) return n",
      "match (n:person) return n",
      "match (n:person) return n",
      "match (n:person) return n",
      "match (n:person) return n")

    var cyphers: Array[String] = Array(

    "match (n) where n.id_p=1 return n limit 1",
    "match (n) where n.id_p=1 return n",
    "match (n) where n.id_p<1 return n limit 1",
    "match (n) where n.id_p<1 return n limit 10",
    "match (n) where n.id_p<1 return n",
    "match (f)-[r]->(t) where f.id_p=1 return count(t)",
    "match (f)-[r:label1]->(t) where f.id_p=1 return count(t)"
    )

    val querys = new QueryTemplate

    //querys.genBatchQuery(10).map(println)

    querys.genBatchQuery(10).map(timing(_)).map(x => println(s"${x._1} cost time: ${x._2}"))


    //var res = graphFacade.cypher("match (n:person) return n")

    //var res2 = cyphers.map(timing(_)).map(x => println(s"${x._1} cost time: ${x._2}"))
  }
}
