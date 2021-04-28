package cn.pandadb.test.cypher.emb

import java.io.File

import cn.pandadb.kernel.store.{PandaNode, PandaRelationship}
import cn.pandadb.kernel.{GraphDatabaseBuilder, GraphService}
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.LynxValue
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: support data structure of cypher
 *              2021-04-26: Int、Long、Boolean、String、Double、Float
 *                          Array[Int]、Array[Long]、Array[Boolean]
 *                          Array[String]、Array[Double]、Array[Float]
 *              future: Blob
 * @author: LiamGao
 * @create: 2021-04-26
 */
class DataStructureTest {
  val dbPath = "./testdata/emb"
  var db: GraphService = _

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File(dbPath))
    FileUtils.forceMkdir(new File(dbPath))
    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath)
  }
  @Test
  def testNode(): Unit ={
    db.cypher(
      """
        |create (n:person:people{
        |name:'alex',
        | money1:100,
        | money2:233.3,
        | flag:true,
        | money11:[11,22,33,44],
        | money22:[22.1, 33.2, 44.3],
        | flags:[true, true, false],
        | jobs:['teacher', 'singer', 'player'],
        | hybridArr:[1, 2.0, "3.0", true]
        | }) return n
        |""".stripMargin).show()

    val res = db.cypher("match (n) return n").records().next()("n").asInstanceOf[PandaNode]

    Assert.assertEquals(100L, res.properties("money1").value)
    Assert.assertEquals(233.3, res.properties("money2").value)
    Assert.assertEquals(true, res.properties("flag").value)
    Assert.assertEquals("alex", res.properties("name").value)
    Assert.assertEquals(Set(11L,22L,33L,44L), res.properties("money11").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(22.1, 33.2, 44.3), res.properties("money22").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(true, true, false), res.properties("flags").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set("teacher", "singer", "player"), res.properties("jobs").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(1, 2.0, "3.0", true), res.properties("hybridArr").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set("person", "people"), res.labels.toSet)
  }

  @Test
  def testRelationship(): Unit ={
    db.cypher(
      """
        |create (n:person1{name:'A'})
        |create (m:person2{name:'B'})
        |""".stripMargin)

    db.cypher(
      """
        |match (n:person1)
        |match (m:person2)
        |create (n)-[r:KNOW{
        |name:'alex',
        | money1:100,
        | money2:233.3,
        | flag:true,
        | money11:[11,22,33,44],
        | money22:[22.1, 33.2, 44.3],
        | flags:[true, true, false],
        | jobs:['teacher', 'singer', 'player'],
        | hybridArr:[1, 2.0, "3.0", true]
        | }]->(m)
        |return r, n, m
        |""".stripMargin).show()

    val res = db.cypher("match (n)-[r]->(m) return r").records().next()("r").asInstanceOf[PandaRelationship]
    Assert.assertEquals(100L, res.properties("money1").value)
    Assert.assertEquals(233.3, res.properties("money2").value)
    Assert.assertEquals(true, res.properties("flag").value)
    Assert.assertEquals("alex", res.properties("name").value)
    Assert.assertEquals(Set(11L,22L,33L,44L), res.properties("money11").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(22.1, 33.2, 44.3), res.properties("money22").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(true, true, false), res.properties("flags").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set("teacher", "singer", "player"), res.properties("jobs").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
    Assert.assertEquals(Set(1, 2.0, "3.0", true), res.properties("hybridArr").value.asInstanceOf[List[LynxValue]].map(f => f.value).toSet)
  }

  @After
  def close(): Unit ={
    db.close()
  }
}
