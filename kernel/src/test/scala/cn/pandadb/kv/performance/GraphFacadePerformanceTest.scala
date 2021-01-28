package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.kv.{GraphFacadeWithPPD, RocksDBStorage}
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.{LynxNull, LynxValue, NodeFilter, RelationshipFilter}
import org.junit.{Before, Test}
import org.opencypher.okapi.api.value.CypherValue.Node
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class GraphFacadePerformanceTest {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {

    val dbPath = "F:\\PandaDB_rocksDB\\ldbc"
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
    graphFacade.createIndexOnNode("person", Set("id"))
    graphFacade.createIndexOnNode("comment", Set("id"))
    graphFacade.createIndexOnNode("post", Set("id"))
    indexStore.allIndexId.foreach(println)
    graphFacade.close()
  }

  @Test
  def createStat(): Unit ={
//    graphFacade.refresh()
    statistics
//    statistics = new Statistics(dbPath)
    indexStore.getIndexIdByLabel(nodeStore.getLabelId("label1")).foreach( s =>println(s.props.head))
  }

  @Test
  def api(): Unit ={
    Profiler.timing({
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false) {
          res += node
          count += 1
        }
      }
    })
    Profiler.timing({
      val label = nodeStore.getLabelId("label1")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == true) {
          res += node
          count += 1
        }
      }
    })
    Profiler.timing({
      val label = nodeStore.getLabelId("label1")
      val prop = nodeStore.getPropertyKeyId("flag")
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == true) {
          res += node
          count += 1
        }
      }
    })
  }

  @Test
  def t(): Unit ={
    val res = graphFacade.cypher("Match (n:label1)  where n.idStr = 'b' return n limit 10")
    res.show()
  }
  @Test
  def testQueryAll(): Unit ={
    graphFacade.cypher("Match (n) where n.idStr='b' return n limit 10 ")
    Profiler.timing(
      {
        val res = graphFacade.cypher("Match (n) where  n.idStr='b' return n limit 10")
        res.show()
      }
    )
//    var res = graphFacade.cypher("Match (n) return n limit 10")
//    res.show()
//    return
//    res = graphFacade.cypher("match ()-[r]->() return r")
//    res.show()
//    res = graphFacade.cypher("match (n:person)-[r]->() return r")
//    res.show()
  }

  @Test
  def testFilterWithSingleProperty(): Unit ={
    var res = graphFacade.cypher("match (n) where n.id_p=1 return n")
    res.show()
    res = graphFacade.cypher("match (n) where n.idStr='a' return n")
    res.show()
    res = graphFacade.cypher("match (n) where n.flag=false return n")
    res.show()
  }
  @Test
  def testFilterWithMultipleProperties(): Unit ={
    var res = graphFacade.cypher("match (n) where n.id_p=1 and n.idStr='a' return n")
    res.show()
    res = graphFacade.cypher("match (n) where n.id_p=1 and n.flag= false return n")
    res.show()
    res = graphFacade.cypher("match (n) where n.idStr='c' and n.flag= false return n")
    res.show()
  }
  @Test
  def testFilterWithLabelAndProperties(): Unit ={
    var res = graphFacade.cypher("match (n:person) return n")
    res.show()
    res = graphFacade.cypher("match (n:person) where n.id_p=1 return n")
    res.show()
    res = graphFacade.cypher("match (n:person) where n.idStr='a' return n")
    res.show()
    res = graphFacade.cypher("match (n:person) where n.flag = false return n")
    res.show()
    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.idStr = 'a' return n")
    res.show()
    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.flag = false return n")
    res.show()
    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.idStr = 'a' and n.flag = false return n")
    res.show()
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

  def LDBC_short1(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
      RelationshipFilter(Seq("isLocatedIn"),Map()), NodeFilter(Seq("place"), Map()),SemanticDirection.OUTGOING
    ).map{
      p =>
        Map(
          "firstName" -> p.startNode.property("firstName"),
          "lastName" -> p.startNode.property("lastName"),
          "birthday" -> p.startNode.property("birthday"),
          "locationIP" -> p.startNode.property("locationIP"),
          "browserUsed" -> p.startNode.property("browserUsed"),
          "cityId" -> p.endNode.property("id"),
          "gender" -> p.startNode.property("gender"),
          "creationDate" -> p.startNode.property("creationDate"),
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short2(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.INCOMING),
      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("post"), Map()),SemanticDirection.OUTGOING),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.OUTGOING),
    ).take(10).map{
      p =>
        Map("messageId" -> p.head.endNode.property("id"),
          "messageCreationDate"-> p.head.endNode.property("creationDate"),
          "originalPostId"->p(1).endNode.property("id"),
          "originalPostAuthorId"->p.last.endNode.property("id"),
          "originalPostAuthorFirstName"->p.last.endNode.property("firstName"),
          "originalPostAuthorLastName"->p.last.endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short3(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
      RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.BOTH
    ).map{
      p =>
        Map(
          "personId" -> p.endNode.property("id"),
          "firstName" -> p.endNode.property("firstName"),
          "lastName" -> p.endNode.property("lastName"),
          "friendshipCreationDate" -> p.storedRelation.property("creationDate")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short4(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.nodes(
      NodeFilter(Seq("comment"),Map("id"->LynxValue(id)))
    ).map{
      n =>
        Map(
          "createDate" -> n.property("creationDate"),
          "content" -> n.property("content")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short5(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
      RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING
    ).map{
      p =>
        Map(
          "personId" -> p.endNode.property("id"),
          "firstName" -> p.endNode.property("firstName"),
          "lastName" -> p.endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short6(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("comment"), Map("id"->LynxValue(id))),
      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("post"), Map()),SemanticDirection.OUTGOING),
      (RelationshipFilter(Seq("containerOf"),Map()), NodeFilter(Seq("forum"), Map()),SemanticDirection.INCOMING),
      (RelationshipFilter(Seq("hasModerator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
    ).map{
      p =>
        Map(
          "forumTitle"-> p.last.startNode.property("title"),
          "forumId"->p.last.startNode.property("id"),
          "moderatorId"->p.last.endNode.property("id"),
          "moderatorFirstName"->p.last.endNode.property("firstName"),
          "moderatorLastName"->p.last.endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def LDBC_short7(id: String): Iterator[Map[String, LynxValue]] ={
    graphFacade.paths(
      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("comment"), Map()),SemanticDirection.INCOMING),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
      (RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.BOTH),
      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("post"), Map("id"->LynxValue(id))),SemanticDirection.INCOMING),
    ).map{
      p =>
        Map(
          "commentId"-> p.head.endNode.property("id"),
          "commentContent"->p.head.endNode.property("content"),
          "commentCreationDate"->p.head.endNode.property("creationDate"),
          "replyAuthorId"->p(1).endNode.property("id"),
          "replyAuthorFirstName"->p(1).endNode.property("firstName"),
          "replyAuthorLastName"->p(1).endNode.property("lastName")
        ).mapValues(_.getOrElse(LynxNull))
    }
  }

  def randomId(array: Array[String]): String = {
    array(Random.nextInt(array.length))
  }


  @Test
  def LDBC(): Unit ={
    val personId = Array("700000002540559", "708796094477119", "719791211845135", "728587303793103", "724189257648007", "700000003592071", "700000001856967", "717592187897287", "719791210631319", "719791211872543", "704398049851831", "700000001884183", "730786326912663", "726388282248071", "706597073343615", "715393163980831", "713194141415295", "724189256634711", "721990233113431", "713194140097367", "708796095517375", "702199023334903", "700000001229487", "717592186099143", "732985348893615", "713194139597231", "704398046691111", "717592186219303", "704398050067911", "710995116332047", "706597073323975", "702199023261503", "713194140404639")

    val postId = Array("787032086215979", "787032086215980", "787032086215981", "787032086215982", "787032086215983", "787032086215984", "787032086215985", "787032086215986", "787032086215987", "716663342038324", "646294597860670", "716663342038354", "787032086216038", "751847714127216", "857400830393722", "681478969949592", "751847714127266", "716663342038444", "892585202482614", "927769574571466", "822216458305000", "822216458305020", "927769574571526", "857400830393872", "927769574571546", "716663342038574", "857400830393912", "892585202482764", "857400830393942", "751847714127456")

    val commentId = Array("827766353710741", "827766353710742", "827766353710743", "827766353710744", "827766353710745", "827766353710746", "827766353710747", "827766353710748", "827766353710749", "827766353710750", "827766353710751", "827766353710752", "827766353710753", "827766353710754", "827766353710755", "827766353710756", "827766353710757", "827766353710758", "827766353710759", "827766353710760", "475922632822442", "475922632822443", "475922632822444", "475922632822445", "475922632822446", "475922632822447", "475922632822448", "475922632822449", "475922632822450", "475922632822451", "475922632822452", "475922632822453", "475922632822454")
    
    val times = 1
    Profiler.timing({
      println("preheat")
      LDBC_short2(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-1.cypher")
      for (i <- 0 until times)
        LDBC_short1(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-2.cypher")
      for (i <- 0 until times)
        LDBC_short2(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-3.cypher")
      for (i <- 0 until times)
        LDBC_short3(randomId(personId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-4.cypher")
      for (i <- 0 until times)
        LDBC_short4(randomId(commentId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-5.cypher")
      for (i <- 0 until times)
        LDBC_short5(randomId(postId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-6.cypher")
      for (i <- 0 until times)
        LDBC_short6(randomId(commentId)).foreach(println)
    })
    Profiler.timing({
      println("interactive-short-7.cypher")
//      for (i <- 0 until times)
//        LDBC_short7(randomId(postId)).foreach(println)
    })

  }

  @Test
  def idTest(): Unit ={
    for (i <- 0 until 201){
      println(nodeStore.newNodeId())
    }
  }

}
