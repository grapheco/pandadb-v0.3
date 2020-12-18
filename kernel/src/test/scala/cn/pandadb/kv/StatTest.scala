package cn.pandadb.kv

import cn.pandadb.kernel.kv.{ByteUtils, RocksDBStorage, StatInfo}
import org.junit.{Assert, Test}

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

  st.save()

  val st2 = new StatInfo(db)

  st2.start()


  @Test
  def testPutAndGet(): Unit = {


    Assert.assertEquals( 10000, st2.allRelCnt)
    Assert.assertEquals(100, st2.allNodesCnt)
    Assert.assertEquals(3, st2.relLabelSchema.size)
    Assert.assertEquals(4, st2.indexPropertySchema.size)
    Assert.assertEquals(3, st2.nodeLabelSchema.size)
    Assert.assertEquals(3, st2.relLabelCnt.size)
    Assert.assertEquals(4, st2.propertyIndexCnt.size)
    Assert.assertEquals(3, st2.labelCnt.size)

    db.close()


  }

  @Test
  def testAddAndDel(): Unit ={
    st2.addNodeLabelCnt("student", 20)
    st2.addNodeLabelCnt("test", 10)
    Assert.assertEquals("student".length + 20, st2.getLabelCnt("student").get)
    Assert.assertEquals(10, st2.getLabelCnt("test").get)
    Assert.assertEquals(130, st2.getAllNodesCnt())

    st2.delNodeLabelCnt("student", 10)
    Assert.assertEquals("student".length + 20 - 10, st2.getLabelCnt("student").get)
    Assert.assertEquals(120, st2.getAllNodesCnt())

    st2.addRelLabelCnt("fans", 20)
    st2.addRelLabelCnt("test", 10)
    Assert.assertEquals("fans".length + 20, st2.getRelLabelCnt("fans").get)
    Assert.assertEquals(10, st2.getRelLabelCnt("test").get)
    Assert.assertEquals(10030, st2.getAllRelCnt())

    st2.delRelLabelCnt("fans", 10)
    Assert.assertEquals("fans".length + 20 - 10, st2.getRelLabelCnt("fans").get)
    Assert.assertEquals(10020, st2.getAllRelCnt())

    st2.addPropertyIndexCnt("student", "name", 10)
    Assert.assertEquals("studentname".length + 10, st2.getPropertyIndexCnt("student","name").get)

    st2.delPropertyIndexCnt("student", "name", 5)
    Assert.assertEquals("studentname".length + 5, st2.getPropertyIndexCnt("student","name").get)



  }

}
