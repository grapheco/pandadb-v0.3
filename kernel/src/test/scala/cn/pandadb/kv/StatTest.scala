package cn.pandadb.kv

import cn.pandadb.kernel.kv.{RocksDBStorage, StatInfo}
import org.junit.Test

class StatTest {
  val db = RocksDBStorage.getDB("./test/info")

  val st = new StatInfo(db)

  Array("student", "person", "paper").foreach(st.nodeLabelSchema+=_)
  Array("knows", "fans", "follows").foreach(st.relLabelSchema+=_)
  Array("studentage", "studentname", "personage", "personname").foreach(st.indexPropertySchema+=_)

  st.nodeLabelSchema.map(u => st.labelCnt +=u->u.getBytes.size.toLong)
  st.relLabelSchema.map(u => st.relLabelCnt +=u->u.getBytes.size.toLong)
  st.indexPropertySchema.map(u => st.propertyIndexCnt +=u->u.getBytes.size.toLong)

  st.allRelCnt = 10000
  st.allNodesCnt = 100

  @Test
  def testPutAndGet(): Unit = {
    st.save()

    val st2 = new StatInfo(db)

    st2.start()

    println(st2.allRelCnt)



  }

}
