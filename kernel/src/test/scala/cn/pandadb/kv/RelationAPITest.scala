package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.junit.{After, Assert, Before, Test}

/**
 * @ClassName RelationAPITest
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/25
 * @Version 0.1
 */
@Test
class RelationAPITest {

  var nodeAPI: NodeStoreSPI = _
  var relationAPI: RelationStoreSPI = _
  val path = "testdata/rocksdb"
  val node1 = new StoredNodeWithProperty(1,Array(1),Map(0->"bob", 1->22, 2-> 66.7, 3->true))
  val node2 = new StoredNodeWithProperty(2,Array(2),Map(0->"tom", 1->24, 2-> 6.7, 3->false))
  val node3 = new StoredNodeWithProperty(3,Array(1),Map(0->"jack", 1->23, 2-> 66.000007, 3->true))
  val node4 = new StoredNodeWithProperty(4,Array(2),Map(0->"jerry", 1->2, 2-> 6.7, 3->false))
  val node5 = new StoredNodeWithProperty(5,Array(1,2),Map(0->"pig", 1->2, 2-> 0.7, 3->true))
  val relation1 = new StoredRelationWithProperty(1, 1, 2, 1, Map(0->3))
  val relation2 = new StoredRelationWithProperty(2, 1, 3, 2, Map(0->3))
  val relation3: StoredRelation = StoredRelation(3, 1, 5, 1)
  val relation4: StoredRelation = StoredRelation(4, 2, 4, 2)
  val relation5: StoredRelation = StoredRelation(5, 3, 2, 1)

  @Before
  def init(): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      dir.delete()
    }
    nodeAPI = new NodeStoreAPI(path)
    relationAPI = new RelationStoreAPI(path)
    nodeAPI.addNode(node1)
    nodeAPI.addNode(node2)
    nodeAPI.addNode(node3)
    nodeAPI.addNode(node4)
    nodeAPI.addNode(node5)
    relationAPI.addRelation(relation1)
    relationAPI.addRelation(relation2)
    relationAPI.addRelation(relation3)
    relationAPI.addRelation(relation4)
    relationAPI.addRelation(relation5)
  }

  @After
  def end(): Unit ={
    nodeAPI.close()
    relationAPI.close()
  }

  @Test
  def getTest(): Unit = {
    Assert.assertArrayEquals(Array[Long](1,2,3,4,5), relationAPI.allRelations().toArray.map(_.id))
    Assert.assertArrayEquals(Array[Long](1,3,5), relationAPI.getRelationIdsByRelationType(1).toArray)
    Assert.assertEquals(1, relationAPI.getRelationById(1).get.typeId)
    Assert.assertArrayEquals(Array[Long](2,3,5), relationAPI.findToNodeIds(1).toArray.sorted)
    Assert.assertArrayEquals(Array[Long](1,3), relationAPI.findFromNodeIds(2).toArray.sorted)
    Assert.assertArrayEquals(Array[Long](2,5), relationAPI.findToNodeIds(1,1).toArray.sorted)
    Assert.assertArrayEquals(Array[Long](1,3), relationAPI.findFromNodeIds(2,1).toArray.sorted)
  }

  @Test
  def deleteTest(): Unit = {
    relationAPI.deleteRelation(1)
    Assert.assertArrayEquals(Array[Long](2,3,4,5), relationAPI.allRelations().toArray.map(_.id))
    Assert.assertArrayEquals(Array[Long](3,5), relationAPI.getRelationIdsByRelationType(1).toArray)
    Assert.assertArrayEquals(Array[Long](3), relationAPI.findFromNodeIds(2).toArray.sorted)
    Assert.assertArrayEquals(Array[Long](5), relationAPI.findToNodeIds(1,1).toArray.sorted)
    Assert.assertArrayEquals(Array[Long](3), relationAPI.findFromNodeIds(2,1).toArray.sorted)
  }

  @Test
  def updateTest(): Unit = {
    Assert.assertEquals(Some(3), relationAPI.getRelationById(1).get.properties.get(0))
    relationAPI.relationSetProperty(1, 0, 1)
    relationAPI.relationSetProperty(1, 1, 1)
    Assert.assertEquals(Some(1), relationAPI.getRelationById(1).get.properties.get(0))
    Assert.assertEquals(Some(1), relationAPI.getRelationById(1).get.properties.get(1))
    Assert.assertEquals(None, relationAPI.getRelationById(5).get.properties.get(0))
    relationAPI.relationSetProperty(5, 0, 3)
    Assert.assertEquals(Some(3), relationAPI.getRelationById(5).get.properties.get(0))
    relationAPI.relationRemoveProperty(1, 1)
    Assert.assertEquals(None, relationAPI.getRelationById(1).get.properties.get(1))
  }

  @Test
  def relationsBetweenTest(): Unit = {
    Assert.assertEquals(relation3.id,
      relationAPI.findOutRelationsBetween(fromNodeId= relation3.from, toNodeId=relation3.to, edgeType=Some(relation3.typeId)).next().id)
    Assert.assertEquals(relation3.id,
      relationAPI.findInRelationsBetween(toNodeId= relation3.to, fromNodeId= relation3.from, edgeType=Some(relation3.typeId)).next().id)

    Assert.assertTrue(relationAPI.findOutRelationsBetween(relation3.from, relation3.to).nonEmpty)
    Assert.assertTrue(relationAPI.findInRelationsBetween(relation3.from, relation3.to).isEmpty)
  }
}
