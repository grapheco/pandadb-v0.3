//import cn.pandadb.kernel.{MetaIdManager, PDBMetaData}
//import cn.pandadb.kernel.util.Profiler.timing
//import org.junit.runners.MethodSorters
//import org.junit.{Assert, FixMethodOrder, Test}
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 12:06 2020/12/25
// * @Modified By:
// */
//
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
//class IdManagerTest {
//  val manager = new MetaIdManager(255)
//
//  // init func test
//  @Test
//  def test1(): Unit = {
//    val id = manager.getId("alice")
//    Assert.assertEquals(id, manager.getId("alice"))
//    val bytes1 = manager.serialized
//    manager.getId("bob")
//    manager.getId("chris")
//    manager.init(bytes1)
//    Assert.assertEquals(1, manager.getId("david"))
//    val bytes2 = manager.serialized
//    manager.init(bytes2)
//    Assert.assertEquals("david", manager.getName(1))
//  }
//
//  @Test
//  def test9(): Unit = {
//    timing(for (i<-1 to 100000000) manager.getId("bob"))
//    timing(for (i<-1 to 100000000) PDBMetaData.getPropId("bob"))
//  }
//
//}
