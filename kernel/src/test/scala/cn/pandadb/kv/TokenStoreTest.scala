//package cn.pandadb.kv
//
//import java.io.File
//
//import cn.pandadb.kernel.kv.meta.NameStore
//import cn.pandadb.kernel.kv.{KeyHandler, RocksDBStorage}
//import org.apache.commons.io.FileUtils
//import org.junit.{After, Assert, Before, Test}
//import org.rocksdb.RocksDB
//
//class TokenStoreTest {
//  var labelStore: LabelStoreTest = _
//
//  @Before
//  def init(): Unit = {
//    FileUtils.deleteDirectory(new File("./testdata/rocksdb"))
//    labelStore = new LabelStoreTest
//  }
//
//  @After
//  def close(): Unit = {
//    labelStore.close()
//  }
//
//  @Test
//  def testIds(): Unit = {
//    labelStore.ids(Set("person", "people", "cat", "dog"))
//    Assert.assertEquals(4, labelStore.mapInt2String.size)
//  }
//
//  @Test
//  def testId(): Unit = {
//    labelStore.id("person")
//    labelStore.id("people")
//    Assert.assertEquals(2, labelStore.mapInt2String.size)
//    labelStore.id("person")
//    Assert.assertEquals(2, labelStore.mapInt2String.size)
//  }
//
//  @Test
//  def testKey(): Unit ={
//    Assert.assertEquals(false, labelStore.key(1).isDefined)
//    labelStore.id("person")
//    Assert.assertEquals(true, labelStore.key(1).isDefined)
//  }
//
//  @Test
//  def testDelete(): Unit = {
//    labelStore.ids(Set("name", "age", "country", "artist"))
//    Assert.assertEquals(4, labelStore.mapInt2String.keySet.size)
//
//    labelStore.delete("age")
//    Assert.assertEquals(3, labelStore.mapInt2String.keySet.size)
//
//    labelStore.id("last")
//    Assert.assertEquals(5, labelStore.idGenerator.get())
//  }
//
//  @Test
//  def testLoadAll(): Unit ={
//    labelStore.ids(Set("name", "age", "country", "artist"))
//    labelStore.close()
//    val labelStore2 = new LabelStoreTest
//    labelStore2.loadAll()
//    Assert.assertEquals(4, labelStore2.mapInt2String.size)
//  }
//
//}
////
////class LabelStoreTest extends NameStore {
////  override val db: RocksDB = {
////    RocksDBStorage.getDB("./testdata/rocksdb")
////  }
////  override val key2ByteArrayFunc: Int => Array[Byte] = KeyHandler.nodeLabelKeyToBytes
////  override val keyPrefixFunc: () => Array[Byte] = KeyHandler.nodeLabelKeyPrefixToBytes
////
////  loadAll()
////}