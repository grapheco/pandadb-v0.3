//package cn.pandadb.kv
//
//import java.io.File
//import java.nio.ByteBuffer
//
//import cn.pandadb.kernel.kv.{KeyConverter, RocksDBStorage}
//import cn.pandadb.kernel.kv.db.KeyValueDB
//import cn.pandadb.kernel.kv.node.NodeStoreAPI
//import cn.pandadb.kernel.util.serializer.NodeSerializer
//import org.lmdbjava.DbiFlags.MDB_CREATE
//import org.junit.{Assert, Before, Test}
//import org.lmdbjava.{Dbi, Env}
//
//import scala.util.Random
//
///**
// * @ClassName LMDBTest
// * @Description TODO
// * @Author huchuan
// * @Date 2021/1/15
// * @Version 0.1
// */
//class LMDBTest {
//  val path = "C:\\LMDB\\nodes"
//  val rocksDBPath = "C:\\PandaDB_rocksDB\\graph500\\nodes"
//  var nodes: KeyValueDB = _
//  var env: Env[ByteBuffer] = _
//  var db:Dbi[ByteBuffer] = _
//  var key:ByteBuffer = _
//  var value:ByteBuffer = _
//
//  @Before
//  def init(): Unit ={
//    nodes = RocksDBStorage.getDB(rocksDBPath)
//    val DB_NAME = "graph500"
//    val dir = new File(path)
//    if (!dir.exists()) {
//      dir.mkdirs()
//    }
//    env = Env.create()
//      .setMapSize(100*1024*1024)
//      .setMaxDbs(1)
//      .open(dir)
//    db = env.openDbi(DB_NAME, MDB_CREATE)
//    key = ByteBuffer.allocateDirect(env.getMaxKeySize)
//    value = ByteBuffer.allocateDirect(1024)
//
//  }
//
//  @Test
//  def importTest(): Unit ={
//
//    try {
//      val txn = env.txnWrite
//      var count = 0
//      var time = System.currentTimeMillis()
//      val iter = nodes.newIterator()
//      iter.seekToFirst()
//      while (iter.isValid){
//        key.put(iter.key()).flip
//        value.put(iter.value()).flip()
//        db.put(txn, key, value)
//        key.clear()
//        value.clear()
//        iter.next()
//        count+=1
//        if(count % 100000 == 0){
//          val time1 = System.currentTimeMillis()
//          println(count, time1 - time)
//          time = time1
//        }
//      }
//      txn.commit()
//      println(count, System.currentTimeMillis() - time)
//    }
//  }
//
//  @Test
//  def seekTest(): Unit = {
//    val nodeStore = new NodeStoreAPI("F:\\PandaDB_rocksDB\\graph500")
//    val keys = new Array[Int](100000).map { i =>
//      val id = Random.nextInt(4000000).toLong
//      nodeStore.getNodeLabelsById(id).headOption.map(
//        label =>
//        KeyConverter.toNodeKey(label, id)
//      )
//    }.filter(_.isDefined).map(_.get)
//    println(keys.length)
//    nodeStore.close()
//    val t1 = System.currentTimeMillis()
//    val tx = env.txnRead()
//    val rocksRsl = keys.map {
//      k =>
//        key.put(k).flip
//        val v = db.get(tx, key)
////        println(v.duplicate().hasArray)
////        val arr = v.array()
////        NodeSerializer.deserializeNodeValue(arr)
//        key.clear()
//    }
//    val t2 = System.currentTimeMillis()
//    println(s"read 100000 nodes cost: ${t2 - t1} ms, hit ${rocksRsl.length} " )
//  }
//}
