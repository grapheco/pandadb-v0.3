//package cn.pandadb.kv
//
//import java.io.File
//
//import cn.pandadb.kernel.kv.RocksDBStorage
//import cn.pandadb.kernel.kv.node.NodeLabelIndex
//import org.junit.{After, Assert, Before, Test}
//import org.rocksdb.RocksDB
//
//import scala.util.Random
//
//class NodeLabelIndexTest {
//
//  var db: RocksDB = null
//  val path = "testdata/rocksdb"
//
//  @Before
//  def init(): Unit = {
//    val dir = new File(path)
//    if (dir.exists()) {
//      dir.delete()
//    }
//    db = RocksDBStorage.getDB(path)
//  }
//  @After
//  def clear(): Unit = {
//    db.close()
//  }
//
//
//  @Test
//  def testForSetGet(): Unit = {
//    val labelIndex = new NodeLabelIndex(db)
//    val labelId: Int = Random.nextInt()
//    val nodeId: Long = Random.nextLong()
//    labelIndex.add(labelId, nodeId)
//
//  }
//
//}
