//package cn.pandadb.kv.performance
//
//import java.io.File
//
//import cn.pandadb.kernel.kv.index.IndexStoreAPI
//import cn.pandadb.kernel.kv.meta.Statistics
//import cn.pandadb.kernel.kv.node.NodeStoreAPI
//import cn.pandadb.kernel.kv.relation.RelationStoreAPI
//import cn.pandadb.kernel.kv.{GraphFacade, RocksDBStorage}
//import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
//import cn.pandadb.kernel.util.Profiler
//import org.apache.commons.io.FileUtils
//import org.grapheco.lynx.{LynxNull, LynxResult, LynxValue, NodeFilter, RelationshipFilter}
//import org.junit.{Before, Test}
//import org.opencypher.v9_0.expressions.SemanticDirection
//
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Random
//
//class GraphFacadePerformanceTests {
//  var nodeStore: NodeStoreSPI = _
//  var relationStore: RelationStoreSPI = _
//  var indexStore: IndexStoreAPI = _
//  var statistics: Statistics = _
//  var graphFacade: GraphFacade = _
//
//
//  @Before
//  def setup(): Unit = {
//
//    val dbPath = "F:\\TD\\ldbc-0.003.panda.db"
//    nodeStore = new NodeStoreAPI(dbPath)
//    relationStore = new RelationStoreAPI(dbPath)
//    indexStore = new IndexStoreAPI(dbPath)
//    statistics = new Statistics(dbPath)
//
//    graphFacade = new GraphFacade(
//      nodeStore,
//      relationStore,
//      indexStore,
//      statistics,
//      {}
//    )
//  }
//
//  @Test
//  def testLabel(): Unit ={
//    nodeStore.allLabels().foreach(println)
//    println("______________")
//    nodeStore.allPropertyKeys().foreach(println)
//    println("______________")
//    relationStore.allPropertyKeys().foreach(println)
//    println("______________")
//    relationStore.allRelationTypes().foreach(println)
//
//  }
//
//  @Test
//  def createIndex(): Unit ={
//    //
//    graphFacade.createIndexOnNode("label1", Set("idStr"))
//    graphFacade.close()
//  }
//
//  @Test
//  def createStat(): Unit ={
//    //    graphFacade.refresh()
//    statistics
//    //    statistics = new Statistics(dbPath)
//    indexStore.getIndexIdByLabel(nodeStore.getLabelId("label1")).foreach( s =>println(s.props.head))
//  }
//
//  @Test
//  def api(): Unit ={
//    Profiler.timing({
//      val label = nodeStore.getLabelId("label0")
//      val prop = nodeStore.getPropertyKeyId("flag")
//      var count = 0
//      val nodes = nodeStore.getNodesByLabel(label)
//      val res = ArrayBuffer[StoredNodeWithProperty]()
//      while (nodes.hasNext && count < 10) {
//        val node = nodes.next()
//        if (node.properties.getOrElse(prop, null) == false) {
//          res += node
//          count += 1
//        }
//      }
//    })
//    Profiler.timing({
//      val label = nodeStore.getLabelId("label1")
//      val prop = nodeStore.getPropertyKeyId("flag")
//      var count = 0
//      val nodes = nodeStore.getNodesByLabel(label)
//      val res = ArrayBuffer[StoredNodeWithProperty]()
//      while (nodes.hasNext && count < 10) {
//        val node = nodes.next()
//        if (node.properties.getOrElse(prop, null) == true) {
//          res += node
//          count += 1
//        }
//      }
//    })
//    Profiler.timing({
//      val label = nodeStore.getLabelId("label1")
//      val prop = nodeStore.getPropertyKeyId("flag")
//      var count = 0
//      val nodes = nodeStore.getNodesByLabel(label)
//      val res = ArrayBuffer[StoredNodeWithProperty]()
//      while (nodes.hasNext && count < 10) {
//        val node = nodes.next()
//        if (node.properties.getOrElse(prop, null) == true) {
//          res += node
//          count += 1
//        }
//      }
//    })
//  }
//
//  @Test
//  def t(): Unit ={
//    val res = graphFacade.cypher("Match (n:label1)  where n.idStr = 'b' return n limit 10")
//    res.show()
//  }
//  @Test
//  def testQueryAll(): Unit ={
//    graphFacade.cypher("Match (n) where n.idStr='b' return n limit 10 ")
//    Profiler.timing(
//      {
//        val res = graphFacade.cypher("Match (n) where  n.idStr='b' return n limit 10")
//        res.show()
//      }
//    )
//    //    var res = graphFacade.cypher("Match (n) return n limit 10")
//    //    res.show()
//    //    return
//    //    res = graphFacade.cypher("match ()-[r]->() return r")
//    //    res.show()
//    //    res = graphFacade.cypher("match (n:person)-[r]->() return r")
//    //    res.show()
//  }
//
//  @Test
//  def testFilterWithSingleProperty(): Unit ={
//    var res = graphFacade.cypher("match (n) where n.id_p=1 return n")
//    res.show()
//    res = graphFacade.cypher("match (n) where n.idStr='a' return n")
//    res.show()
//    res = graphFacade.cypher("match (n) where n.flag=false return n")
//    res.show()
//  }
//  @Test
//  def testFilterWithMultipleProperties(): Unit ={
//    var res = graphFacade.cypher("match (n) where n.id_p=1 and n.idStr='a' return n")
//    res.show()
//    res = graphFacade.cypher("match (n) where n.id_p=1 and n.flag= false return n")
//    res.show()
//    res = graphFacade.cypher("match (n) where n.idStr='c' and n.flag= false return n")
//    res.show()
//  }
//  @Test
//  def testFilterWithLabelAndProperties(): Unit ={
//    var res = graphFacade.cypher("match (n:person) return n")
//    res.show()
//    res = graphFacade.cypher("match (n:person) where n.id_p=1 return n")
//    res.show()
//    res = graphFacade.cypher("match (n:person) where n.idStr='a' return n")
//    res.show()
//    res = graphFacade.cypher("match (n:person) where n.flag = false return n")
//    res.show()
//    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.idStr = 'a' return n")
//    res.show()
//    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.flag = false return n")
//    res.show()
//    res = graphFacade.cypher("match (n:person) where n.id_p=1 and n.idStr = 'a' and n.flag = false return n")
//    res.show()
//  }
//
//
//  def timing(cy: String): (String, Long) = {
//    val t1 = System.currentTimeMillis()
//    graphFacade.cypher(cy)
//    val t2 = System.currentTimeMillis()
//    cy->(t2-t1)
//  }
//
//  @Test
//  def testTime(): Unit = {
//    var cyphers1: Array[String] = Array("match (n:person) return n",
//      "match (n) return n",
//      "match (n:person) return n",
//      "match (n:person) where n.name = 'joe' return n",
//      "match (n:student) where n.age = 100 return n",
//      "match (n:person) return n",
//      "match (n:person) return n",
//      "match (n:person) return n",
//      "match (n:person) return n",
//      "match (n:person) return n",
//      "match (n:person) return n",
//      "match (n:person) return n",
//      "match (n:person) return n")
//
//    var cyphers: Array[String] = Array(
//
//      "match (n) where n.id_p=1 return n limit 1",
//      "match (n) where n.id_p=1 return n",
//      "match (n) where n.id_p<1 return n limit 1",
//      "match (n) where n.id_p<1 return n limit 10",
//      "match (n) where n.id_p<1 return n",
//      "match (f)-[r]->(t) where f.id_p=1 return count(t)",
//      "match (f)-[r:label1]->(t) where f.id_p=1 return count(t)"
//    )
//
//    val querys = new QueryTemplate
//
//    //querys.genBatchQuery(10).map(println)
//
//    querys.genBatchQuery(10).map(timing(_)).map(x => println(s"${x._1} cost time: ${x._2}"))
//
//
//    //var res = graphFacade.cypher("match (n:person) return n")
//
//    //var res2 = cyphers.map(timing(_)).map(x => println(s"${x._1} cost time: ${x._2}"))
//  }
//
//  def LDBC_short1(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
//      RelationshipFilter(Seq("isLocatedIn"),Map()), NodeFilter(Seq("place"), Map()),SemanticDirection.OUTGOING
//    ).map{
//      p =>
//        Map(
//          "firstName" -> p.startNode.property("firstName"),
//          "lastName" -> p.startNode.property("lastName"),
//          "birthday" -> p.startNode.property("birthday"),
//          "locationIP" -> p.startNode.property("locationIP"),
//          "browserUsed" -> p.startNode.property("browserUsed"),
//          "cityId" -> p.endNode.property("id"),
//          "gender" -> p.startNode.property("gender"),
//          "creationDate" -> p.startNode.property("creationDate"),
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def LDBC_short2(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
//      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.INCOMING),
//      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("post"), Map()),SemanticDirection.OUTGOING),
//      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.OUTGOING),
//    ).take(10).map{
//      p =>
//        Map("messageId" -> p.head.endNode.property("id"),
//          "messageCreationDate"-> p.head.endNode.property("creationDate"),
//          "originalPostId"->p(1).endNode.property("id"),
//          "originalPostAuthorId"->p.last.endNode.property("id"),
//          "originalPostAuthorFirstName"->p.last.endNode.property("firstName"),
//          "originalPostAuthorLastName"->p.last.endNode.property("lastName")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def LDBC_short3(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("person"), Map("id"->LynxValue(id))),
//      RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq(), Map()),SemanticDirection.BOTH
//    ).map{
//      p =>
//        Map(
//          "personId" -> p.endNode.property("id"),
//          "firstName" -> p.endNode.property("firstName"),
//          "lastName" -> p.endNode.property("lastName"),
//          "friendshipCreationDate" -> p.storedRelation.property("creationDate")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def LDBC_short4(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.nodes(
//      NodeFilter(Seq("comment"),Map("id"->LynxValue(id)))
//    ).map{
//      n =>
//        Map(
//          "createDate" -> n.property("creationDate"),
//          "content" -> n.property("content")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def LDBC_short5(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
//      RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING
//    ).map{
//      p =>
//        Map(
//          "personId" -> p.endNode.property("id"),
//          "firstName" -> p.endNode.property("firstName"),
//          "lastName" -> p.endNode.property("lastName")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def LDBC_short6(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("comment"), Map("id"->LynxValue(id))),
//      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("post"), Map()),SemanticDirection.OUTGOING),
//      (RelationshipFilter(Seq("containerOf"),Map()), NodeFilter(Seq("forum"), Map()),SemanticDirection.INCOMING),
//      (RelationshipFilter(Seq("hasModerator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
//    ).map{
//      p =>
//        Map(
//          "forumTitle"-> p.last.startNode.property("title"),
//          "forumId"->p.last.startNode.property("id"),
//          "moderatorId"->p.last.endNode.property("id"),
//          "moderatorFirstName"->p.last.endNode.property("firstName"),
//          "moderatorLastName"->p.last.endNode.property("lastName")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def LDBC_short7(id: String): Iterator[Map[String, LynxValue]] ={
//    graphFacade.paths(
//      NodeFilter(Seq("post"), Map("id"->LynxValue(id))),
//      (RelationshipFilter(Seq("replyOf"),Map()), NodeFilter(Seq("comment"), Map()),SemanticDirection.INCOMING),
//      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.OUTGOING),
//      (RelationshipFilter(Seq("knows"),Map()), NodeFilter(Seq("person"), Map()),SemanticDirection.BOTH),
//      (RelationshipFilter(Seq("hasCreator"),Map()), NodeFilter(Seq("post"), Map("id"->LynxValue(id))),SemanticDirection.INCOMING),
//    ).map{
//      p =>
//        Map(
//          "commentId"-> p.head.endNode.property("id"),
//          "commentContent"->p.head.endNode.property("content"),
//          "commentCreationDate"->p.head.endNode.property("creationDate"),
//          "replyAuthorId"->p(1).endNode.property("id"),
//          "replyAuthorFirstName"->p(1).endNode.property("firstName"),
//          "replyAuthorLastName"->p(1).endNode.property("lastName")
//        ).mapValues(_.getOrElse(LynxNull))
//    }
//  }
//
//  def randomId(array: Array[String]): String = {
//    array(Random.nextInt(array.length))
//  }
//
//
//  @Test
//  def LDBC(): Unit ={
//    val personId = Array("119791209300010", "126388279066632", "126388279066641", "110995116277761", "100000000000014", "100000000000016", "104398046511148", "115393162788899", "126388279066650", "121990232555526", "121990232555527", "100000000000027", "115393162788910", "110995116277782", "115393162788912", "100000000000033", "110995116277783", "126388279066664", "132985348833291", "100000000000047", "128587302322180", "102199023255557", "132985348833319", "128587302322191", "128587302322196")
//    val postId = Array("601030792151557", "601030792151574", "600962072674855", "601030792152544", "600687194768871", "600962072675836", "601030792152581", "600962072675856", "601030792152797", "600618475292937", "600755914246416", "601030792153367", "600687194769694", "600687194769708", "601030792154125", "601030792154136", "601030792154880", "600893353201425", "600755914247970", "600962072678195", "600755914248005", "600755914248015", "600755914248025", "600755914248035", "600755914248045", "600755914248055", "600893353201829", "601030792155817", "600962072680060", "600893353203917")
//    val commentId = Array("801030792151558", "801030792151559", "801030792151560", "801030792151561", "801030792151562", "801030792151563", "801030792151564", "801030792151565", "801030792151566", "801030792151567", "801030792151568", "801030792151569", "801030792151570", "801030792151571", "801030792151572", "801030792151573", "801030792151575", "801030792151576", "801030792151577", "801030792151578", "801030792151579", "801030792151580", "801030792151581", "801030792151582", "801030792151583", "801030792151584", "801030792151585", "801030792151586", "801030792151587", "801030792151588", "801030792151589", "801030792151590", "800962072674856", "800962072674857", "800962072674858", "800962072674859", "800962072674860", "800962072674861", "800962072674862", "800962072674863", "800962072674864", "800962072674865", "800962072674866", "800962072674867", "800962072674868", "800962072674869", "800962072674870", "800962072674871", "801030792152545", "801030792152546", "801030792152547", "801030792152548", "801030792152549", "801030792152550", "800687194768872", "800687194768873", "800687194768874", "800687194768875", "800687194768876", "800687194768877", "800687194768878", "800687194768879", "800687194768880", "800687194768881")
//    val times = 10
//    Profiler.timing({
//      println("preheat")
//      LDBC_short2(randomId(personId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-1.cypher")
//      for (i <- 0 until times)
//        LDBC_short1(randomId(personId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-2.cypher")
//      for (i <- 0 until times)
//        LDBC_short2(randomId(personId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-3.cypher")
//      for (i <- 0 until times)
//        LDBC_short3(randomId(personId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-4.cypher")
//      for (i <- 0 until times)
//        LDBC_short4(randomId(commentId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-5.cypher")
//      for (i <- 0 until times)
//        LDBC_short5(randomId(postId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-6.cypher")
//      for (i <- 0 until times)
//        LDBC_short6(randomId(commentId)).foreach(println)
//    })
//    Profiler.timing({
//      println("interactive-short-7.cypher")
//      //      for (i <- 0 until times)
//      //        LDBC_short7(randomId(postId)).foreach(println)
//    })
//
//  }
//
//  def LDBC_short1_Cypher(myid:String): LynxResult ={
//    val ldbc_short1 = s"MATCH (n:person {id:'$myid'})-[:isLocatedIn]->(p:place) RETURN n.firstName AS firstName, n.lastName AS lastName,  n.birthday AS birthday,  n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS cityId,  n.gender AS gender, n.creationDate AS creationDate"
//    graphFacade.cypher(ldbc_short1)
//  }
//
//  def LDBC_short2_Cypher(myid:String): LynxResult ={
//    val ldbc_short2 = s"MATCH (:person {id:'$myid'})<-[:hasCreator]-(m)-[:replyOf]->(p:post) MATCH (p)-[:hasCreator]->(c) RETURN  m.id AS messageId,  m.creationDate AS messageCreationDate,  p.id AS originalPostId,  c.id AS originalPostAuthorId,  c.firstName AS originalPostAuthorFirstName,  c.lastName AS originalPostAuthorLastName LIMIT 10"
//    graphFacade.cypher(ldbc_short2)
//  }
//  def LDBC_short3_Cypher(myid:String): LynxResult ={
//    val ldbc_short3 = s"MATCH (n:person {id:'$myid'})-[r:knows]-(friend)\nRETURN\n  friend.id AS personId,\n  friend.firstName AS firstName,\n  friend.lastName AS lastName,\n  r.creationDate AS friendshipCreationDate"
//    graphFacade.cypher(ldbc_short3)
//  }
//  def LDBC_short4_Cypher(myid:String): LynxResult ={
//    val ldbc_short4 = s"MATCH (m:comment {id:'$myid'})\nRETURN\n  m.creationDate AS messageCreationDate,\n  m.content as content"
//    graphFacade.cypher(ldbc_short4)
//  }
//  def LDBC_short5_Cypher(myid:String): LynxResult ={
//    val ldbc_short5 = s"MATCH (m:post {id:'$myid'})-[:hasCreator]->(p:person)\nRETURN\n  p.id AS personId,\n  p.firstName AS firstName,\n  p.lastName AS lastName"
//    graphFacade.cypher(ldbc_short5)
//  }
//  def LDBC_short6_Cypher(myid:String): LynxResult ={
//    val ldbc_short6 = s"MATCH (m:comment{id:'$myid'})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)\nRETURN\n  f.id AS forumId,\n  f.title AS forumTitle,\n  mod.id AS moderatorId,\n  mod.firstName AS moderatorFirstName,\n  mod.lastName AS moderatorLastName"
//    graphFacade.cypher(ldbc_short6)
//  }
//  def LDBC_short7_Cypher(myid:String): LynxResult ={
//    val ldbc_short7 = s"MATCH (m:post{id:'$myid'})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)\nMATCH (m)-[:hasCreator]->(a:person)-[r:knows]-(p)\nRETURN\n  c.id AS commentId,\n  c.content AS commentContent,\n  c.creationDate AS commentCreationDate,\n  p.id AS replyAuthorId,\n  p.firstName AS replyAuthorFirstName,\n  p.lastName AS replyAuthorLastName"
//    graphFacade.cypher(ldbc_short7)
//  }
//  @Test
//  def testQ(): Unit ={
//
//    val personIds = Array("119791209300010", "126388279066632", "126388279066641", "110995116277761", "100000000000014", "100000000000016", "104398046511148", "115393162788899", "126388279066650", "121990232555526", "121990232555527", "100000000000027", "115393162788910", "110995116277782", "115393162788912", "100000000000033", "110995116277783", "126388279066664", "132985348833291", "100000000000047", "128587302322180", "102199023255557", "132985348833319", "128587302322191", "128587302322196")
//    val postIds = Array("601030792151557", "601030792151574", "600962072674855", "601030792152544", "600687194768871", "600962072675836", "601030792152581", "600962072675856", "601030792152797", "600618475292937", "600755914246416", "601030792153367", "600687194769694", "600687194769708", "601030792154125", "601030792154136", "601030792154880", "600893353201425", "600755914247970", "600962072678195", "600755914248005", "600755914248015", "600755914248025", "600755914248035", "600755914248045", "600755914248055", "600893353201829", "601030792155817", "600962072680060", "600893353203917")
//    val commentIds = Array("801030792151558", "801030792151559", "801030792151560", "801030792151561", "801030792151562", "801030792151563", "801030792151564", "801030792151565", "801030792151566", "801030792151567", "801030792151568", "801030792151569", "801030792151570", "801030792151571", "801030792151572", "801030792151573", "801030792151575", "801030792151576", "801030792151577", "801030792151578", "801030792151579", "801030792151580", "801030792151581", "801030792151582", "801030792151583", "801030792151584", "801030792151585", "801030792151586", "801030792151587", "801030792151588", "801030792151589", "801030792151590", "800962072674856", "800962072674857", "800962072674858", "800962072674859", "800962072674860", "800962072674861", "800962072674862", "800962072674863", "800962072674864", "800962072674865", "800962072674866", "800962072674867", "800962072674868", "800962072674869", "800962072674870", "800962072674871", "801030792152545", "801030792152546", "801030792152547", "801030792152548", "801030792152549", "801030792152550", "800687194768872", "800687194768873", "800687194768874", "800687194768875", "800687194768876", "800687194768877", "800687194768878", "800687194768879", "800687194768880", "800687194768881")
//
//    val personId: String = randomId(personIds)
//    val commentId: String = randomId(commentIds)
//    val postId: String = randomId(postIds)
//    // val ldbc_short1 = s"MATCH (n:person {id:'$personId'})-[:isLocatedIn]->(p:place) RETURN n "
//
//    val ldbc_short1 = s"MATCH (n:person {id:'$personId'})-[:isLocatedIn]->(p:place) RETURN n.firstName AS firstName, n.lastName AS lastName,  n.birthday AS birthday,  n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS cityId,  n.gender AS gender, n.creationDate AS creationDate"
//    val ldbc_short2 = s"MATCH (:person {id:'$personId'})<-[:hasCreator]-(m)-[:replyOf]->(p:post) MATCH (p)-[:hasCreator]->(c) RETURN  m.id AS messageId,  m.creationDate AS messageCreationDate,  p.id AS originalPostId,  c.id AS originalPostAuthorId,  c.firstName AS originalPostAuthorFirstName,  c.lastName AS originalPostAuthorLastName LIMIT 10"
//    val ldbc_short3 = s"MATCH (n:person {id:'$personId'})-[r:knows]-(friend)\nRETURN\n  friend.id AS personId,\n  friend.firstName AS firstName,\n  friend.lastName AS lastName,\n  r.creationDate AS friendshipCreationDate"
//    val ldbc_short4 = s"MATCH (m:comment {id:'$commentId'})\nRETURN\n  m.creationDate AS messageCreationDate,\n  m.content as content"
//    val ldbc_short5 = s"MATCH (m:post {id:'$postId'})-[:hasCreator]->(p:person)\nRETURN\n  p.id AS personId,\n  p.firstName AS firstName,\n  p.lastName AS lastName"
//    val ldbc_short6 = s"MATCH (m:comment{id:'$commentId'})-[:replyOf]->(p:post)<-[:containerOf]-(f:forum)-[:hasModerator]->(mod:person)\nRETURN\n  f.id AS forumId,\n  f.title AS forumTitle,\n  mod.id AS moderatorId,\n  mod.firstName AS moderatorFirstName,\n  mod.lastName AS moderatorLastName"
//    val ldbc_short7 = s"MATCH (m:post{id:'$postId'})<-[:replyOf]-(c:comment)-[:hasCreator]->(p:person)\nMATCH (m)-[:hasCreator]->(a:person)-[r:knows]-(p)\nRETURN\n  c.id AS commentId,\n  c.content AS commentContent,\n  c.creationDate AS commentCreationDate,\n  p.id AS replyAuthorId,\n  p.firstName AS replyAuthorFirstName,\n  p.lastName AS replyAuthorLastName"
//
//
//    val api1 = LDBC_short1(personId)
//    val api2 = LDBC_short2(personId)
//    val api3 = LDBC_short3(personId)
//    val api4 = LDBC_short4(commentId)
//    val api5 = LDBC_short5(postId)
//    val api6 = LDBC_short6(commentId)
//    val api7 = LDBC_short7(postId)
//
//    val cy = ldbc_short2
//    val myAI = api2
//
//    val t0 = System.currentTimeMillis()
//    val res0 = graphFacade.cypher(cy)
//    val t1 = System.currentTimeMillis()
//    val res = graphFacade.cypher(cy)
//    val t2 = System.currentTimeMillis()
//    println(res.records().size)
//    val t3 = System.currentTimeMillis()
//    myAI.foreach(_.map(x => println(x._1,x._2.value)))
//    val t4 = System.currentTimeMillis()
//    println(s"heat time ${t1-t0}")
//    println(s"cypher time ${t2-t1}")
//    println(s"show time ${t3-t2}")
//    println(s"cyAll time ${t3-t1}")
//    println(s"API time ${t4-t3}")
//
//    // println(s"sjdksjdk '$personId'")
//  }
//
//  def runCypher(i: Int,time: Int): Unit ={
//    val personIds = Array("119791209300010", "126388279066632", "126388279066641", "110995116277761", "100000000000014", "100000000000016", "104398046511148", "115393162788899", "126388279066650", "121990232555526", "121990232555527", "100000000000027", "115393162788910", "110995116277782", "115393162788912", "100000000000033", "110995116277783", "126388279066664", "132985348833291", "100000000000047", "128587302322180", "102199023255557", "132985348833319", "128587302322191", "128587302322196")
//    val postIds = Array("601030792151557", "601030792151574", "600962072674855", "601030792152544", "600687194768871", "600962072675836", "601030792152581", "600962072675856", "601030792152797", "600618475292937", "600755914246416", "601030792153367", "600687194769694", "600687194769708", "601030792154125", "601030792154136", "601030792154880", "600893353201425", "600755914247970", "600962072678195", "600755914248005", "600755914248015", "600755914248025", "600755914248035", "600755914248045", "600755914248055", "600893353201829", "601030792155817", "600962072680060", "600893353203917")
//    val commentIds = Array("801030792151558", "801030792151559", "801030792151560", "801030792151561", "801030792151562", "801030792151563", "801030792151564", "801030792151565", "801030792151566", "801030792151567", "801030792151568", "801030792151569", "801030792151570", "801030792151571", "801030792151572", "801030792151573", "801030792151575", "801030792151576", "801030792151577", "801030792151578", "801030792151579", "801030792151580", "801030792151581", "801030792151582", "801030792151583", "801030792151584", "801030792151585", "801030792151586", "801030792151587", "801030792151588", "801030792151589", "801030792151590", "800962072674856", "800962072674857", "800962072674858", "800962072674859", "800962072674860", "800962072674861", "800962072674862", "800962072674863", "800962072674864", "800962072674865", "800962072674866", "800962072674867", "800962072674868", "800962072674869", "800962072674870", "800962072674871", "801030792152545", "801030792152546", "801030792152547", "801030792152548", "801030792152549", "801030792152550", "800687194768872", "800687194768873", "800687194768874", "800687194768875", "800687194768876", "800687194768877", "800687194768878", "800687194768879", "800687194768880", "800687194768881")
//
//    i match {
//      case 1 => {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(personIds)).map(_._2)
//        Ids.foreach(LDBC_short1_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short1_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short1(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//      case 2 =>
//      {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(personIds)).map(_._2)
//        Ids.foreach(LDBC_short2_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short2_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short2(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//      case 3 =>
//      {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(personIds)).map(_._2)
//        Ids.foreach(LDBC_short3_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short3_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short3(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//      case 4 =>
//      {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(commentIds)).map(_._2)
//        Ids.foreach(LDBC_short4_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short4_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short4(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//      case 5 =>
//      {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(postIds)).map(_._2)
//        Ids.foreach(LDBC_short5_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short5_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short5(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//      case 6 =>
//      {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(commentIds)).map(_._2)
//        Ids.foreach(LDBC_short6_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short6_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short6(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//      case 7 =>
//      {
//        val Ids:Array[String] = (1 to time).toArray.map(x => x-> randomId(postIds)).map(_._2)
//        Ids.foreach(LDBC_short7_Cypher(_))
//        val t1 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short7_Cypher(_).records().size)
//        val t2 = System.currentTimeMillis()
//        Ids.foreach(LDBC_short7(_).size)
//        val t3 = System.currentTimeMillis()
//        println(s"cypher cost time:${t2-t1}  API cost time: ${t3-t2}")
//      }
//    }
//  }
//
//  @Test
//  def testTT(): Unit = {
//    runCypher(2, 10)
//  }
//
//}
