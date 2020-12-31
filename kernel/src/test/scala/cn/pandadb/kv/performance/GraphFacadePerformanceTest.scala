package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI}
import cn.pandadb.kernel.util.Profiler
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

    val dbPath = "D:\\data\\yiyidata\\db"
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
  }

  @Test
  def testLabel(): Unit ={
    nodeStore.allLabels().foreach(println)
    println("______________")
    nodeStore.allPropertyKeys().foreach(println)
    println("______________")
    relationStore.allPropertyKeys().foreach(println)
    println("______________")
    relationStore.allRelationTypes().foreach(println)

  }

  @Test
  def createIndex(): Unit ={
    // 
    graphFacade.createIndexOnNode("label1", Set("idStr"))
  }

  @Test
  def createStat(): Unit ={
//    graphFacade.refresh()
//    statistics
    indexStore.getIndexIdByLabel(nodeStore.getLabelId("label1")).foreach( s =>println(s._1(0)))
  }

  @Test
  def t(): Unit ={
    val res = graphFacade.cypher("Match (n:label1)  where n.idStr = 'b' return n limit 10")
    res.show
  }
  @Test
  def testQueryAll(): Unit ={
    graphFacade.cypher("Match (n) where n.idStr='b' return n limit 10 ")
    Profiler.timing(
      {
        val res = graphFacade.cypher("Match (n) where  n.idStr='b' return n limit 10")
        res.show
      }
    )
//    var res = graphFacade.cypher("Match (n) return n limit 10")
//    res.show
//    return
//    res = graphFacade.cypher("match ()-[r]->() return r")
//    res.show
//    res = graphFacade.cypher("match (n:person)-[r]->() return r")
//    res.show
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
