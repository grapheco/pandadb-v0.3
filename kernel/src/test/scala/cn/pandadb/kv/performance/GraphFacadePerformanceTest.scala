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
//import org.grapheco.lynx.{LynxNull, LynxValue, NodeFilter, RelationshipFilter}
//import org.junit.{Before, Test}
//import org.opencypher.v9_0.expressions.SemanticDirection
//
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Random
//
//class GraphFacadePerformanceTest {
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
//    val dbPath = "C:\\ldbc-1.panda.db"
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
//    graphFacade.createIndexOnNode("person", Set("id"))
//    graphFacade.createIndexOnNode("comment", Set("id"))
//    graphFacade.createIndexOnNode("post", Set("id"))
//    indexStore.allIndexId.foreach(println)
//    graphFacade.close()
//  }
//
//  @Test
//  def createStat(): Unit ={
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
//    "match (n) where n.id_p=1 return n limit 1",
//    "match (n) where n.id_p=1 return n",
//    "match (n) where n.id_p<1 return n limit 1",
//    "match (n) where n.id_p<1 return n limit 10",
//    "match (n) where n.id_p<1 return n",
//    "match (f)-[r]->(t) where f.id_p=1 return count(t)",
//    "match (f)-[r:label1]->(t) where f.id_p=1 return count(t)"
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
//    val personId = Array("808796093032109", "832985348833869", "819791209309325", "817592186046389", "800000000002037", "821990232560629", "830786325580725", "815393162798957", "819791209301429", "804398046518165", "806597069776093", "821990232559621", "800000000001029", "815393162791429", "828587302328333", "806597069768549", "804398046513797", "832985348843478", "819791209306110", "813194139535870", "813194139536910", "806597069767694", "828587302327110", "828587302330662", "810995116281166", "806597069773206", "830786325578646", "804398046512118", "806597069775054", "815393162799310", "824189255820966", "832985348838886", "824189255817062", "810995116279774", "808796093025118", "806597069767342", "808796093025454", "810995116283118", "828587302328118", "800000000001198", "821990232557646", "832985348837510", "817592186046558", "810995116286982", "813194139534998", "806597069775742", "824189255813878", "800000000007414", "817592186053446", "802199023263478", "813194139542190", "832985348838718", "824189255816374", "824189255818390", "804398046517190", "813194139542558", "815393162792470", "819791209300774")
//
//    val postId = Array("202061586740819", "200412319299172", "200687197206133", "201236953020055", "200962075113128", "201924147787449", "200549758252746", "201511830927067", "200274880345836", "201786708834045", "200549758252814", "201511830927135", "201374391973680", "201236953020225", "200824636159826", "201511830927203", "200824636159860", "201374391973765", "200962075113366", "201236953020327", "201924147787704", "201649269880777", "200549758253035", "200687197206524", "202061586741261", "200274880346142", "200824636160047", "202061586741312", "201374391973969", "202061586741346", "200549758253188", "201786708834453", "200412319299750", "201649269881015", "201924147787976", "201236953020633", "201924147788010", "201099514067195", "201511830927628", "201924147788061", "201374391974190", "200549758253375", "200274880346448", "200824636160370", "200412319299971", "201649269881236", "202061586741669", "201786708834759", "200824636160472", "202061586741737", "201786708834810", "201511830927883", "200274880346652", "200824636160557", "200962075114046", "201099514067535", "200412319300192", "201236953021041", "200274880346771")
//
//    val commentId = Array("101099512344703", "101786707112065", "101786707112066", "101786707112067", "101786707112068", "101786707112069", "101786707112070", "101786707112071", "101786707112072", "101786707112073", "101786707112074", "101786707112075", "101786707112076", "101786707112077", "101786707112078", "100962073391263", "100962073391264", "100962073391265", "100962073391266", "100962073391267", "100962073391268", "100962073391269", "100962073391270", "100962073391271", "100962073391272", "100962073391273", "100962073391274", "100962073391275", "100962073391276", "101924146065612", "101924146065613", "102061585019086", "102061585019087", "102061585019088", "101924146065617", "102061585019090", "102061585019091", "101924146065620", "102061585019093")
//
//
//    val times = 1
//    Profiler.timing({
//      println("preheat")
//      LDBC_short2(randomId(personId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-1.cypher")
//      for (i <- 0 until times)
//        LDBC_short1(randomId(personId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-2.cypher")
//      for (i <- 0 until times)
//        LDBC_short2(randomId(personId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-3.cypher")
//      for (i <- 0 until times)
//        LDBC_short3(randomId(personId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-4.cypher")
//      for (i <- 0 until times)
//        LDBC_short4(randomId(commentId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-5.cypher")
//      for (i <- 0 until times)
//        LDBC_short5(randomId(postId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-6.cypher")
//      for (i <- 0 until times)
//        LDBC_short6(randomId(commentId)).toArray
//    })
//    Profiler.timing({
//      println("interactive-short-7.cypher")
//      for (i <- 0 until times)
//        LDBC_short7(randomId(postId)).foreach(println)
//    })
//
//  }
//
//  @Test
//  def idTest(): Unit ={
//    for (i <- 0 until 201){
//      println(nodeStore.newNodeId())
//    }
//  }
//
//}
