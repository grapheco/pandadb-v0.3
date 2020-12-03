package cn.pandadb.kv.performance

import java.io.File

import cn.pandadb.kernel.kv.{NodeStore, RelationStore, RocksDBStorage}
import org.junit.Before
import org.rocksdb.RocksDB

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:44 2020/12/1
 * @Modified By:
 */
class WritePERFTest {
  val nodeFile = "D://dataset//graph500-22_unique_node";
  val edgeFile = "D://dataset//graph500-22";
  val dbPath = "C://rocksDB//performanceTest";
  var db: RocksDB = null
  var nodeStore: NodeStore = null
  var relationStore: RelationStore = null

  @Before
  def init(): Unit ={

    val dir = new File(dbPath)
      if (dir.exists()) {
        _deleteDir(dir)
      }
    db = RocksDBStorage.getDB(dbPath)
    nodeStore = new NodeStore(db)
    relationStore = new RelationStore(db)
  }

  private def _deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        _deleteDir(f)
      } else {
        f.delete()
      }
    })
    dir.delete()
  }

  private def _loadAllNodes(iter: Iterator[String]): Unit = {
    iter.foreach(str => {
      nodeStore.set(str.toLong, Array(1,2,3), Map("idid" -> str))
    })
  }

  private def _loadAllEdges(iter: Iterator[String]): Unit = {
    iter.foreach(str => {
      val rvArr: Array[String] = str.split("\t")
      val id1: Long = rvArr(0).toLong
      val id2: Long = rvArr(1).toLong
      relationStore.setRelation(id1, id1, id2, 1, id1, Map[String, Any]("relProp" -> "test rel"))
    })
  }
}
