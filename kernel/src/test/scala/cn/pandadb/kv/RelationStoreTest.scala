package cn.pandadb.kv

import cn.pandadb.kernel.kv.{KeyHandler, RelationStore}
import org.junit.{After, Assert, Before, Test}

class RelationStoreTest {
  var relationStore: RelationStore = null
  var keyIn: Array[Byte] = null
  var keyOut: Array[Byte] = null

  val jsonString = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"}," +
    "\"time\":[{\"arrayKey\":\"arrayVal\"},{\"key2\":\"val2\"}]}"

  @Before
  def init(): Unit ={
    relationStore = new RelationStore
    keyIn = KeyHandler.inEdgeKeyToBytes(1,1,1,1)
    keyOut = KeyHandler.outEdgeKeyToBytes(1,1,1,1)
    relationStore.putRelation(1,1,1,1, jsonString)
  }

  @After
  def close(): Unit ={
    relationStore.close()
  }

  @Test
  def writeAndGetTest(): Unit ={
   val resIn = relationStore.getRelationValueObject(keyIn).toString
    val resOut = relationStore.getRelationValueObject(keyOut).toString()

    Assert.assertEquals(resIn, resOut)
  }

  @Test
  def isExistTest(): Unit ={
    relationStore.putRelation(1,1,1,1, jsonString)
    val resIn = relationStore.relationIsExist(keyIn)
    val resOut = relationStore.relationIsExist(keyOut)

    Assert.assertEquals(true, resIn)
    Assert.assertEquals(true, resOut)
  }

  @Test
  def updateValue(): Unit ={
    val jsonString2 = "{\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"}," +
      "\"time\":[{\"arrayKey\":\"arrayVal\"},{\"key2\":\"val2\"}]}"

    relationStore.updateRelation(keyIn, jsonString2)

    val res1 = relationStore.getRelationValueObject(keyIn).toString()
    val res2 = relationStore.getRelationValueObject(keyOut).toString()

    Assert.assertEquals(false, res1.contains("et"))
    Assert.assertEquals(false, res2.contains("et"))
  }

  @Test
  def delete(): Unit ={
    relationStore.deleteRelation(keyIn)

    val resIn = relationStore.relationIsExist(keyIn)
    val resOut = relationStore.relationIsExist(keyOut)
    Assert.assertEquals(false, resIn)
    Assert.assertEquals(false, resOut)
  }

  @Test
  def iterator(): Unit ={
    val iter = relationStore.getAllRelation(KeyHandler.KeyType.InEdge.id.toByte, 1)

    Assert.assertEquals(2, iter.toStream.length)
  }
}
