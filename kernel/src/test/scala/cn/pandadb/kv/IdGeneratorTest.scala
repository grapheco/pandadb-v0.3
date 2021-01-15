//package cn.pandadb.kv
//
//import java.io.File
//
//import cn.pandadb.kernel.kv.RocksDBStorage
//import cn.pandadb.kernel.kv.meta.{NodeIdGenerator, RelationIdGenerator}
//import org.apache.commons.io.FileUtils
//import org.junit.{After, Assert, Before, Test}
//import org.rocksdb.RocksDB
//
//
//class NodeIdGeneratorTest {
//
//  var db: RocksDB = null
//  var nodeIdGenerator: NodeIdGenerator = null
//
//  @Before
//  def init(): Unit = {
//    FileUtils.deleteDirectory(new File("./testdata"))
//    new File("./testdata").mkdirs()
//    val dbPath = "./testdata/id"
//    db = RocksDBStorage.getDB(dbPath)
//    nodeIdGenerator = new NodeIdGenerator(db)
//  }
//
//  @After
//  def close(): Unit = {
//    nodeIdGenerator.flush()
//    db.close()
//  }
//
//
//  @Test
//  def testNextId(): Unit = {
//    println(nodeIdGenerator.nextId())
//    for(i <- 1L to 1000L) {
//      Assert.assertEquals(i, nodeIdGenerator.nextId())
//    }
//  }
//
//  @Test
//  def testFlush(): Unit = {
//    val maxId = 34903908
//    for(i:Long <- 1L to maxId) {
//      Assert.assertEquals(i, nodeIdGenerator.nextId())
//    }
//    close()
//    new File("./testdata").mkdirs()
//    val dbPath = "./testdata/id"
//    db = RocksDBStorage.getDB(dbPath)
//    nodeIdGenerator = new NodeIdGenerator(db)
//    Assert.assertTrue(nodeIdGenerator.nextId()>maxId)
//  }
//}
//
//class RelationIdGeneratorTest {
//
//  var db: RocksDB = null
//  var relationIdGenerator: RelationIdGenerator = null
//
//  @Before
//  def init(): Unit = {
//    FileUtils.deleteDirectory(new File("./testdata"))
//    new File("./testdata").mkdirs()
//    val dbPath = "./testdata/id"
//    db = RocksDBStorage.getDB(dbPath)
//    relationIdGenerator = new RelationIdGenerator(db)
//  }
//
//  @After
//  def close(): Unit = {
//    relationIdGenerator.flush()
//    db.close()
//  }
//
//
//  @Test
//  def testNextId(): Unit = {
//    for(i <- 1L to 1000L) {
//      Assert.assertEquals(i, relationIdGenerator.nextId())
//    }
//  }
//
//  @Test
//  def testFlush(): Unit = {
//    val maxId = 34903908
//    for(i:Long <- 1L to maxId) {
//      Assert.assertEquals(i, relationIdGenerator.nextId())
//    }
//    close()
//    new File("./testdata").mkdirs()
//    val dbPath = "./testdata/id"
//    db = RocksDBStorage.getDB(dbPath)
//    relationIdGenerator = new RelationIdGenerator(db)
//    Assert.assertTrue(relationIdGenerator.nextId()>maxId)
//  }
//
//
//}
//
