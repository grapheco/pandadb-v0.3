package cn.pandadb.kv.distributed

import cn.pandadb.kernel.distribute.DistributedGraphFacade
import cn.pandadb.kernel.store.StoredNodeWithProperty
import org.grapheco.lynx.{LynxInteger, LynxString}
import org.junit.{After, Assert, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-17 18:06
 */
class DistributedGraphFacadeTest {

  val api = new DistributedGraphFacade()

  @Before
  def init(): Unit ={
//    api.indexStore.cleanIndexes(api.nodeIndex, api.relationIndex, api.propertyIndex)
//    addData()
  }

  def addData(): Unit ={
    api.addNode(Map("name"->"glx1", "age"->11), "person", "worker")
    api.addNode(Map("name"->"glx2", "age"->12, "country"->"China"), "person", "human")
    api.addNode(Map("name"->"glx3", "age"->13), "person", "CNIC")
    api.addNode(Map("name"->"glx4", "age"->14), "person", "bass player")
    api.addNode(Map("name"->"glx5", "age"->15), "person", "man")

    api.addRelation("friend1", 1, 2, Map.empty)
    api.addRelation("friend2", 1, 3, Map.empty)
    api.addRelation("friend3", 1, 4, Map.empty)
    api.addRelation("friend4", 4, 5, Map("Year"->2020))
  }

  @Test
  def scanAllNodes(): Unit ={
    api.scanAllNode().foreach(println)
  }

  @Test
  def deleteNode(): Unit ={
    api.deleteNode(2)
    Assert.assertEquals(None, api.getNode(2))
  }
  @Test
  def nodeAddLabel(): Unit ={
    api.nodeAddLabel(1, "test")
    Assert.assertEquals(Seq("person", "worker", "test"), api.getNode(1).get.labels)
  }
  @Test
  def nodeRemoveLabel(): Unit ={
    api.nodeRemoveLabel(1, "test")
    Assert.assertEquals(Seq("person", "worker"), api.getNode(1).get.labels)
  }
  @Test
  def nodeSetProperty(): Unit ={
    api.nodeSetProperty(1, "TestKey", "testValue")
    Assert.assertEquals(Seq(("name", LynxString("glx1")),("age", LynxInteger(11)),("TestKey", LynxString("testValue"))),
      api.getNode(1).get.properties.toSeq)
  }
  @Test
  def nodeRemoveProperty(): Unit ={
    api.nodeRemoveProperty(1, "TestKey")
    Assert.assertEquals(Seq(("name", LynxString("glx1")),("age", LynxInteger(11))),
      api.getNode(1).get.properties.toSeq)
  }

  @Test
  def allRelations(): Unit ={
    api.scanAllRelations().foreach(println)
  }
  @Test
  def relationSetProperty(): Unit ={
    api.relationSetProperty(1, "Color", "blue")
    Assert.assertEquals(Map("Color"->LynxString("blue")), api.getRelation(1).get.properties)
  }

  @Test
  def relationRemoveProperty(): Unit = {
    api.relationRemoveProperty(1, "Color")
    Assert.assertEquals(Map.empty, api.getRelation(1).get.properties)
  }

  @Test
  def cypher1(): Unit ={
    api.cypher("match (n) return n").show()
  }
  @Test
  def cypher2(): Unit ={
    api.cypher("match (n)-[r]-(m) return r").show()
  }

  @After
  def close(): Unit ={
    api.close() // flush id to db
  }
}
