//package cn.pandadb.kv
//
//import cn.pandadb.kernel.kv.{KeyHandler, RelationStore, RocksDBStorage}
//import org.junit.{After, Assert, Before, Test}
//
//class RelationStoreTest {
//  var relationStore: RelationStore = null
//  var keyIn: Array[Byte] = null
//  var keyOut: Array[Byte] = null
//
//  @Before
//  def init(): Unit = {
//    val db = RocksDBStorage.getDB()
//    relationStore = new RelationStore(db)
//    keyIn = KeyHandler.inEdgeKeyToBytes(1, 1, 1, 1)
//    keyOut = KeyHandler.outEdgeKeyToBytes(1, 1, 1, 1)
//    relationStore.putRelation(1, 1, 1, 1, Map[String, Object]("a" -> 1.asInstanceOf[Object], "b" -> "c".asInstanceOf[Object]))
//  }
//
//  @After
//  def close(): Unit = {
//    relationStore.close()
//  }
//
//  @Test
//  def writeAndGetTest(): Unit = {
//    val resIn = relationStore.getRelationValueMap(keyIn).toString()
//    val resOut = relationStore.getRelationValueMap(keyOut).toString()
//
//    Assert.assertEquals(resIn, resOut)
//  }
//
//  @Test
//  def isExistTest(): Unit = {
//    val resIn = relationStore.relationIsExist(keyIn)
//    val resOut = relationStore.relationIsExist(keyOut)
//
//    Assert.assertEquals(true, resIn)
//    Assert.assertEquals(true, resOut)
//  }
//
//  @Test
//  def updateValue(): Unit = {
//    relationStore.updateRelation(keyIn, Map[String, Object]("ccc"->222.asInstanceOf[Object]))
//
//    val res1 = relationStore.getRelationValueMap(keyIn)
//    val res2 = relationStore.getRelationValueMap(keyOut)
//
//    Assert.assertEquals(Set("ccc"), res1.keySet)
//    Assert.assertEquals(Set("ccc"), res2.keySet)
//  }
//
//  @Test
//  def delete(): Unit = {
//    relationStore.deleteRelation(keyIn)
//
//    val resIn = relationStore.relationIsExist(keyIn)
//    val resOut = relationStore.relationIsExist(keyOut)
//    Assert.assertEquals(false, resIn)
//    Assert.assertEquals(false, resOut)
//  }
//
//  @Test
//  def iterator(): Unit = {
//    val iter = relationStore.getAllRelation(KeyHandler.KeyType.InEdge.id.toByte, 1)
//
//    Assert.assertEquals(2, iter.toStream.length)
//  }
//
//  @Test
//  def mapTest(): Unit = {
//    val map = Map[String, Object]("a" -> 1.asInstanceOf[Object], "b" -> "c".asInstanceOf[Object])
//    val bt = relationStore.map2ArrayByte(map)
//    val getMap = relationStore.arrayByte2Map(bt)
//
//    Assert.assertEquals(1, getMap("a"))
//    Assert.assertEquals("c", getMap("b"))
//  }
//}
