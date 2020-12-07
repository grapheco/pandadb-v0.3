package cn.pandadb.kv

import cn.pandadb.kernel.{NodeId, TypedId}
import cn.pandadb.kernel.kv.{NodeFulltextIndex, RocksDBStorage}
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

@Test
class NodeFulltextIndexTest extends Assert {

  val path = "D:\\PandaDB-tmp"
  var db: RocksDB = null
  var index: NodeFulltextIndex = null
  var data: Iterator[(TypedId, Map[String, String])] = Map[TypedId, Map[String, String]](
      NodeId(1)->Map("name" -> "张三"),
      NodeId(2)->Map("name" -> "张三丰"),
      NodeId(3)->Map("name" -> "李四光"),
      NodeId(4)->Map("name" -> "王五刀"),
      NodeId(5)->Map("name" -> "Scala"),
      NodeId(6)->Map("name" -> "PandaDB"),
      NodeId(7)->Map("desc" -> "PandaDB is a Intelligent Graph Database.")
    ).iterator
  var label = 5
  var props = Array[Int](5)

  @Before
  def init: Unit ={
    db = RocksDBStorage.getDB(path+"/rocksdb")
    index = NodeFulltextIndex(db, path+"/lucene",label, props)
    index.dropIfExists()
    index.createIfNotExists()
    index.open()
  }

  @Test
  def fulltext = {
    // create and insert
    index.insert(data)
    // search
    println(index.find((Array("name"), "张三")).toList)
    Assert.assertArrayEquals(Array[Long](1, 2), index.find((Array("name"), "张三")).map(m=>{m.get("id").get.asInstanceOf[Long]}).toArray)
    Assert.assertArrayEquals(Array[Long](6, 7), index.find((Array("name","desc"), "PandaDB")).map(m=>{m.get("id").get.asInstanceOf[Long]}).toArray)
    Assert.assertArrayEquals(Array[Long](4), index.find((Array("name"), "王五刀")).map(m=>{m.get("id").get.asInstanceOf[Long]}).toArray)
  }

  @After
  def cleanup: Unit ={
    index.close()
    db.close()
  }
}