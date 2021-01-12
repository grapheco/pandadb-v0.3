//package cn.pandadb
//
//import java.io.File
//
//import cn.pandadb.kernel.store.FileBasedIdGen
//import org.junit.{Assert, Before, Test}
//
///**
//  * Created by bluejoe on 2020/6/4.
//  */
//class IdGenTest {
//  @Before
//  def setup(): Unit = {
//    val file = new File("./testdata/output/idgen")
//    if (file.exists())
//      file.delete()
//
//    file.createNewFile()
//  }
//
//  @Test
//  def test1(): Unit = {
//    val file = new File("./testdata/output/idgen")
//    val idgen = new FileBasedIdGen(file, 10)
//    Assert.assertEquals(0, idgen.currentId())
//    Assert.assertEquals(1, idgen.nextId())
//    Assert.assertEquals(2, idgen.nextId())
//    idgen.flush()
//    Assert.assertEquals(2, idgen.currentId())
//    Assert.assertEquals(3, idgen.nextId())
//
//    idgen.flush()
//    //now shutdown
//
//    val idgen2 = new FileBasedIdGen(file, 10)
//    Assert.assertEquals(3, idgen2.currentId())
//    Assert.assertEquals(4, idgen2.nextId())
//
//    //shutdown without no flush
//    val idgen3 = new FileBasedIdGen(file, 10)
//    Assert.assertEquals(13, idgen3.currentId())
//    Assert.assertEquals(14, idgen3.nextId())
//
//    //multi-threads
//    val threads = (1 to 5).map(_ =>
//      new Thread() {
//        override def run(): Unit = {
//          val cid = idgen3.nextId()
//          println(cid)
//        }
//      })
//
//    threads.foreach(_.start)
//    threads.foreach(_.join())
//
//    Assert.assertEquals(14 + 5, idgen3.currentId())
//  }
//
//  @Test
//  def test2(): Unit = {
//    val file = new File("./testdata/output/idgen")
//    val idgen = new FileBasedIdGen(file, 10)
//    (1 to 5).foreach(_ => idgen.nextId())
//    Assert.assertEquals(5, idgen.currentId())
//    try {
//      idgen.update(3)
//      Assert.assertFalse(true)
//    }
//    catch {
//      case e =>
//        Assert.assertTrue(true)
//    }
//
//    idgen.update(8)
//    Assert.assertEquals(8, idgen.currentId())
//
//    //now shutdown without flush
//    val idgen2 = new FileBasedIdGen(file, 10)
//    Assert.assertEquals(10, idgen2.currentId())
//
//    idgen2.update(88)
//    Assert.assertEquals(88, idgen2.currentId())
//    val idgen3 = new FileBasedIdGen(file, 10)
//    Assert.assertEquals(97, idgen3.currentId())
//  }
//}
