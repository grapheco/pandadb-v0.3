package cn.pandadb.test.api.graphfacade

import java.io.File
import cn.pandadb.kernel.kv.GraphFacade
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}

import scala.{Array, Float}
import scala.collection.immutable.Range.Double
import scala.io.{BufferedSource, Source}

class IndexApi {
  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacade = _
  /*
    longString
   */
  val longTextFile: BufferedSource = Source.fromFile(new File("./testdata/longText"))
  val longText: String = longTextFile.getLines().next()

  /*
    label names
   */
  val LABEL1 = "person"
  val LABEL2 = "student"

  /*
    prop names
   */
  val PROP1 = "name"
  val PROP2 = "age"
  val PROP3 = "man"

  /*
    test dataset for integerIndexTest.
    _integerTestData: a list of integer prop value.
    _intRangeTestData: (start, end, withStart, withEnd, result)
   */
  val _integerTestData: List[Any] = List(0,1,2,0,1,2,0,-1,-10,-100,100,99,
    Int.MaxValue - 1, Int.MaxValue, Int.MinValue, Int.MinValue + 1,
    Long.MaxValue, Long.MinValue, 12345678998765L)
  val _intRangeTestData: List[(Long, Long, Boolean, Boolean, List[Any])] = List(
    (0, 2, true, true, List(0,0,0,1,1,2,2)),
    (0, 2, false, true, List(1,1,2,2)),
    (0, 2, true, false, List(0,0,0,1,1)),
    (0, 2, false, false, List(1,1)),
    (-99, 100, true, true, List(-10,-1,0,0,0,1,1,2,2,99,100)),
    (-99, 100, false, true, List(-10,-1,0,0,0,1,1,2,2,99,100)),
    (-99, 100, true, false, List(-10,-1,0,0,0,1,1,2,2,99)),
    (-99, 100, false, false, List(-10,-1,0,0,0,1,1,2,2,99)),
    (Int.MinValue, Int.MaxValue, true, true, List(Int.MinValue,Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1,Int.MaxValue)),
    (Int.MinValue, Int.MaxValue, false, true, List(Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1,Int.MaxValue)),
    (Int.MinValue, Int.MaxValue, true, false, List(Int.MinValue,Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1)),
    (Int.MinValue, Int.MaxValue, false, false, List(Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1)),
    (Long.MinValue, Long.MaxValue, true, true, List(Long.MinValue, Int.MinValue,Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1,Int.MaxValue,12345678998765L,Long.MaxValue)),
    (Long.MinValue, Long.MaxValue, false, true, List(Int.MinValue,Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1,Int.MaxValue,12345678998765L,Long.MaxValue)),
    (Long.MinValue, Long.MaxValue, true, false, List(Long.MinValue, Int.MinValue,Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1,Int.MaxValue,12345678998765L)),
    (Long.MinValue, Long.MaxValue, false, false, List(Int.MinValue,Int.MinValue + 1,-100,-10,-1,0,0,0,1,1,2,2,99,100,Int.MaxValue-1,Int.MaxValue,12345678998765L)),
  )

  /*
    test dataset for floatIndexTest
    _floatTestData: a list of float prop value.
    _floatRangeTestData: (start, end, withStart, withEnd, result)
   */
  val _floatTestData: List[Double] = List(0.0, 0.01, 0.001, 0.000001, 1.0/3, 1.0,
    99.9999, 100.0, -99.9999, -99.9997)
  val _floatRangeTestData: List[(Double, Double, Boolean, Boolean, List[Double])] = List(
    (0.0, 1.0, true, true, List(0.0, 0.000001, 0.001,0.01, 1.0/3, 1.0)),
    (0.0, 1.0, false, true, List(0.000001, 0.001,0.01, 1.0/3, 1.0)),
    (0.0, 1.0, true, false, List(0.0, 0.000001, 0.001,0.01, 1.0/3)),
    (0.0, 1.0, false, false, List(0.000001, 0.001,0.01, 1.0/3)),
    (-99.9998, 100.0, true, true, List(-99.9997, 0.0, 0.000001, 0.001,0.01, 1.0/3, 1.0, 99.9999, 100.0)),
    (-99.9998, 100.0, false, true, List(-99.9997, 0.0, 0.000001, 0.001,0.01, 1.0/3, 1.0, 99.9999, 100.0)),
    (-99.9998, 100.0, true, false, List(-99.9997, 0.0, 0.000001, 0.001,0.01, 1.0/3, 1.0, 99.9999)),
    (-99.9998, 100.0, false, false, List(-99.9997, 0.0, 0.000001, 0.001,0.01, 1.0/3, 1.0, 99.9999)),
  )

  /*
    test dataset for stringIndexTest.
    _stringTestData: a list of string prop value.
    _startWithTestData: a list of (startString, numbers)
   */
  val _stringTestData: List[String] = List("alex", "alex0", "alice", "bob", "zoo",
    "alex", "alex0", "alice", "bob", "zoo", "张三",
    "张三丰", "000", "0", longText, longText + "1")
  val _startWithTestData: Map[String, Int] = Map("a" -> 6, "al"-> 6, "alex" -> 4,
  "alex0" -> 2, "z"->2, "0"->2, "张"->2, "张三丰"->1, longText->2)


  @Before
  def init(): Unit = {
    val dbPath = "./testdata/testdb/test.db"

    FileUtils.deleteDirectory(new File("./testdata/testdb"))
    FileUtils.forceMkdir(new File(dbPath))

    nodeStore = new NodeStoreAPI(dbPath)
    relationStore = new RelationStoreAPI(dbPath)
    indexStore = new IndexStoreAPI(dbPath)
    statistics = new Statistics(dbPath)

    graphFacade = new GraphFacade(
      nodeStore,
      relationStore,
      indexStore,
      statistics,
      {}
    )
  }

  // TODO statistics
  @Test
  def nodeIndexTest(): Unit ={
    var indexes = indexStore.allIndexId
    Assert.assertEquals(indexes.length, 0)

    val bob = graphFacade.addNode(Map(PROP1->"bob", PROP2->23), LABEL1)
    val alex = graphFacade.addNode(Map(PROP1->"alex", PROP3->true), LABEL2)
    val tom = graphFacade.addNode(Map(PROP1->"tom", PROP2->25, PROP3->true), LABEL1, LABEL2)

    // create index
    graphFacade.createIndexOnNode(LABEL1, Set(PROP1))
    graphFacade.createIndexOnNode(LABEL1, Set(PROP2))
    graphFacade.createIndexOnNode(LABEL2, Set(PROP1))
    graphFacade.createIndexOnNode(LABEL2, Set(PROP3))
    // create mistake index
    graphFacade.createIndexOnNode(LABEL1, Set("email")) // with unknown prop
    graphFacade.createIndexOnNode("teacher", Set(PROP1)) // with unknown label

    indexes = indexStore.allIndexId
    Assert.assertEquals(6, indexes.length)

    Assert.assertEquals(1, graphFacade.findNodeByIndex(
      graphFacade.hasIndex(Set(LABEL1), Set(PROP1)).get._1, "bob").length)
    Assert.assertEquals(2, graphFacade.findNodeByIndex(
      graphFacade.hasIndex(Set(LABEL2), Set(PROP3)).get._1, true).length)

    graphFacade.addNode(Map(PROP1->"bob", PROP2->25, PROP3->true), LABEL1, LABEL2)

    Assert.assertEquals(2, graphFacade.findNodeByIndex(
      graphFacade.hasIndex(Set(LABEL1), Set(PROP1)).get._1, "bob").length)
    Assert.assertEquals(3, graphFacade.findNodeByIndex(
      graphFacade.hasIndex(Set(LABEL2), Set(PROP3)).get._1, true).length)

    // drop a index
    indexStore.dropIndex(nodeStore.getLabelId(LABEL1).get, Array(nodeStore.getPropertyKeyId(PROP1).get))
    Assert.assertEquals(5, indexStore.allIndexId.length)
    // drop again
    indexStore.dropIndex(nodeStore.getLabelId(LABEL1).get, Array(nodeStore.getPropertyKeyId(PROP1).get))
    Assert.assertEquals(5, indexStore.allIndexId.length)
    // drop a not exist index
    indexStore.dropIndex(nodeStore.getLabelId(LABEL1).get, Array(nodeStore.getPropertyKeyId(PROP3).get))
    Assert.assertEquals(5, indexStore.allIndexId.length)
  }

  @Test
  def integerIndexTest(): Unit ={
    // insert node
    val nodes = _integerTestData.map(
      int => graphFacade.addNode(Map(PROP1 -> int), LABEL1) -> int).toMap
    // create index
    graphFacade.createIndexOnNode(LABEL1, Set(PROP1))
    val index = graphFacade.hasIndex(Set(LABEL1), Set(PROP1))
    Assert.assertTrue(index.nonEmpty)
    Assert.assertEquals(index.get._2, LABEL1)
    Assert.assertEquals(index.get._3, Set(PROP1))
    Assert.assertEquals(index.get._4, nodes.size)
    val indexId = index.get._1
    // search
    _integerTestData.toSet.toArray.foreach{
      int =>
        val res = graphFacade.findNodeByIndex(indexId, int).toArray
        Assert.assertEquals(res.length, _integerTestData.count(int.equals))
        Assert.assertTrue(res.map(_.longId).forall(nodes(_).equals(int)))
    }
    // rangeSearch
    _intRangeTestData.foreach(
      test =>
        Assert.assertArrayEquals(
          indexStore.findIntegerRange(indexId, test._1, test._2, test._3, test._4).map(nodes).toArray.map(_.toString.toLong),
          test._5.map(_.toString.toLong).toArray
        )
    )
  }

  @Test
  def floatIndexTest(): Unit ={
    // insert node
    val nodes = _floatTestData.map(
      float => graphFacade.addNode(Map(PROP1 -> float), LABEL1) -> float).toMap
    // create index
    graphFacade.createIndexOnNode(LABEL1, Set(PROP1))
    val index = graphFacade.hasIndex(Set(LABEL1), Set(PROP1))
    Assert.assertTrue(index.nonEmpty)
    Assert.assertEquals(index.get._2, LABEL1)
    Assert.assertEquals(index.get._3, Set(PROP1))
    Assert.assertEquals(index.get._4, nodes.size)
    val indexId = index.get._1
    // search
    _floatTestData.toSet.toArray.foreach{
      float =>
        val res = graphFacade.findNodeByIndex(indexId, float).toArray
        Assert.assertEquals(res.length, _floatTestData.count(float.equals))
        Assert.assertTrue(res.map(_.longId).forall(nodes(_).equals(float)))
    }
    // rangeSearch
    _floatRangeTestData.foreach (
      test =>
        Assert.assertArrayEquals(
          indexStore.findFloatRange(indexId, test._1, test._2, test._3, test._4).map(nodes).toArray,
          test._5.toArray, 0.000000001
        )
    )
  }

  @Test
  def stringIndexTest(): Unit ={
    // insert node
    val nodes = _stringTestData.map(str => graphFacade.addNode(Map(PROP1->str), LABEL1) -> str).toMap
    // create index
    graphFacade.createIndexOnNode(LABEL1, Set(PROP1))
    val index = graphFacade.hasIndex(Set(LABEL1), Set(PROP1))
    Assert.assertTrue(index.nonEmpty)
    Assert.assertEquals(index.get._2, LABEL1)
    Assert.assertEquals(index.get._3, Set(PROP1))
    Assert.assertEquals(index.get._4, nodes.size)
    val indexId = index.get._1
    // search
    _stringTestData.toSet.toArray.foreach{
      str =>
        val res = graphFacade.findNodeByIndex(indexId, str).toArray
        Assert.assertEquals(res.length, _stringTestData.count(str.equals))
        Assert.assertTrue(res.map(_.longId).forall(nodes(_).equals(str)))
    }
    // startWith
    _startWithTestData.foreach{
      stringCount =>
        val res = indexStore.findStringStartWith(indexId, stringCount._1).toArray
        Assert.assertEquals(res.length, stringCount._2)
        Assert.assertTrue(res.forall(nodes(_).startsWith(stringCount._1)))
    }
  }

  @Test
  def mixIndexTest(): Unit ={
    val _mixTestData: List[Any] = List("alex", "alice", "alex", 123, -99, 0, 1.89)
    // insert node
    val nodes = _mixTestData.map(str => graphFacade.addNode(Map(PROP1->str), LABEL1) -> str).toMap
    // create index
    graphFacade.createIndexOnNode(LABEL1, Set(PROP1))
    val index = graphFacade.hasIndex(Set(LABEL1), Set(PROP1))
    Assert.assertTrue(index.nonEmpty)
    Assert.assertEquals(index.get._2, LABEL1)
    Assert.assertEquals(index.get._3, Set(PROP1))
    Assert.assertEquals(index.get._4, nodes.size)
    val indexId = index.get._1
    // search
    _mixTestData.toSet.toArray.foreach{
      str =>
        val res = graphFacade.findNodeByIndex(indexId, str).toArray
        Assert.assertEquals(res.length, _mixTestData.count(str.equals))
        Assert.assertTrue(res.map(_.longId).forall(nodes(_).equals(str)))
    }
    // startWith
    Map[String, Int]("al"-> 3,
      "alex" -> 2,
      "1" -> 0
    ).foreach{
      stringCount =>
        val res = indexStore.findStringStartWith(indexId, stringCount._1).toArray
        Assert.assertEquals(res.length, stringCount._2)
        Assert.assertTrue(res.forall(nodes(_).isInstanceOf[String]))
        Assert.assertTrue(res.forall(nodes(_).asInstanceOf[String].startsWith(stringCount._1)))
    }
    // range
    Assert.assertArrayEquals(
      indexStore.findIntegerRange(indexId, -100, 100, startClosed = true, endClosed = true)
        .map(nodes(_).asInstanceOf[Int]).toArray, Array(-99,0))
    Assert.assertArrayEquals(
      indexStore.findFloatRange(indexId, -100.0, 100.0, startClosed = true, endClosed = true)
        .map(nodes(_).asInstanceOf[Double]).toArray, Array(1.89), 0.00000001)
  }

  @Test
  def fulltextIndexTest(): Unit ={
    val bob = graphFacade.addNode(Map(PROP1->"bob", PROP2->23), LABEL1)
    val alex = graphFacade.addNode(Map(PROP1->"alex", PROP3->true), LABEL2)
    val tom = graphFacade.addNode(Map(PROP1->"bob", PROP2->25, PROP3->true), LABEL1, LABEL2)
    val indexId = indexStore.createIndex(
      nodeStore.getLabelId(LABEL1).get,
      Array(nodeStore.getPropertyKeyId(PROP1).get),
      true)
    val prop1 = nodeStore.getPropertyKeyId(PROP1).get
    indexStore.insertFulltextIndexRecordBatch(indexId,
      graphFacade.getNodesByLabels(Seq(LABEL1), false).map(
        n=>(Array(prop1).map(n.properties), n.id)
      ))
    Assert.assertEquals(2, indexStore.search(indexId, Array(prop1), "bob").length)
  }

  @Test
  def compositeIndexTest(): Unit ={
    //TODO test compositeIndex
  }

  @After
  def close(): Unit = {
    graphFacade.close()
  }
}
