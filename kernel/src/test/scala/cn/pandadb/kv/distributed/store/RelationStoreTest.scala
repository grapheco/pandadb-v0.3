package cn.pandadb.kv.distributed.store

import java.nio.ByteBuffer

import cn.pandadb.kernel.distribute.PandaDistributeKVAPI
import cn.pandadb.kernel.distribute.meta.PropertyNameStore
import cn.pandadb.kernel.distribute.relationship.{DistributedRelationStoreSPI, RelationStoreAPI}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import org.junit.{After, Assert, Before, Test}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient
import org.tikv.shade.com.google.protobuf.ByteString

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-18 16:35
 */
class RelationStoreTest {
  var api: DistributedRelationStoreSPI = _
  var tikv: RawKVClient = _

  val r1 = new StoredRelationWithProperty(1, 1, 2, 1, Map(1 -> "tom1", 234 -> 666))
  val r2 = new StoredRelationWithProperty(2, 1, 3, 3, Map(1 -> "tom2"))
  val r3 = new StoredRelationWithProperty(3, 1, 4, 1, Map(1 -> "tom3"))
  val r4 = new StoredRelationWithProperty(4, 2, 3, 2, Map(1 -> "tom4"))
  val r5 = new StoredRelationWithProperty(5, 2, 4, 2, Map(1 -> "tom5"))
  val r6 = new StoredRelationWithProperty(6, 3, 4, 2, Map(1 -> "tom6"))

  @Before
  def init(): Unit = {
    val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
    val session = TiSession.create(conf)
    tikv = session.createRawClient()

    cleanDB()

    val db = new PandaDistributeKVAPI(tikv)

    api = new RelationStoreAPI(db, new PropertyNameStore(db))

    api.addRelation(r1)
    api.addRelation(r2)
    api.addRelation(r3)
    api.addRelation(r4)
    api.addRelation(r5)
    api.addRelation(r6)
  }

  def cleanDB(): Unit ={
    val left = ByteString.copyFrom(ByteBuffer.wrap(Array((0).toByte)))
    val right = ByteString.copyFrom(ByteBuffer.wrap(Array((-1).toByte)))
    tikv.deleteRange(left, right)
  }

  @Test
  def all(): Unit = {
    val array = Array(r1, r2, r3, r4, r5, r6)

    api.allRelations().zipWithIndex.foreach(ri => {
      Assert.assertEquals(array(ri._2), ri._1)
    })
  }

  @Test
  def getRelationByType(): Unit = {
    val array = Array(r4, r5, r6)
    api.getRelationIdsByRelationType(2).zipWithIndex.foreach(
      ii => {
        Assert.assertEquals(array(ii._2).id, ii._1)
      }
    )
  }

  @Test
  def delete(): Unit = {
    api.deleteRelation(r4.id)
    val array = Array(r1, r2, r3, r5, r6)

    api.allRelations().zipWithIndex.foreach(ri => {
      Assert.assertEquals(array(ri._2), ri._1)
    })
  }

  @Test
  def setProperty(): Unit = {
    api.relationSetProperty(1, 233, "rocks rocks oh")
    Assert.assertEquals(r1.properties ++ Map(233 -> "rocks rocks oh"), api.getRelationById(1).get.properties)
  }

  @Test
  def removeProperty(): Unit = {
    api.relationRemoveProperty(1, 1)
    Assert.assertEquals(r1.properties - 1, api.getRelationById(1).get.properties)
  }

  @Test
  def findOutRelations(): Unit = {
    Assert.assertEquals(List(r1, r2, r3), api.findOutRelations(1).toList.sortBy(f => f.id))
  }

  @Test
  def findInRelations(): Unit = {
    Assert.assertEquals(List(r6.invert()), api.findInRelations(3).toList)
  }

  @Test
  def findTo(): Unit = {
    Assert.assertEquals(List(2, 3, 4), api.findToNodeIds(1).toList.sortBy(f => f))
  }

  @Test
  def findIn(): Unit = {
    Assert.assertEquals(List(4), api.findFromNodeIds(3).toList)
  }


  @After
  def close(): Unit = {
    api.close()
  }
}
