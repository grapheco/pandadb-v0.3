import java.io.File

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.RocksDBGraphAPI
import org.junit.{Assert, FixMethodOrder, Test}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:08 2020/12/3
 * @Modified By:
 */
@FixMethodOrder
class PDBMetaDataTest {
  val pdbMetaData = PDBMetaData
  pdbMetaData.addProp("name")
  val mmap: Map[String, Int] = Map[String, Int]("name" -> 0)
  val persistFile: File = new File("./src/test/output/metadata.bin")
  val dbPath: String = "./src/test/output/testdb"
  val api: RocksDBGraphAPI = new RocksDBGraphAPI(dbPath)

  @Test
  def getPerformace(): Unit = {
    val time0 = System.currentTimeMillis()
    for(i<-1 to 100000000){
      val propId: Int = mmap("name")
    }
    val time1 = System.currentTimeMillis()

    for(i<-1 to 100000000){
      val propId: Int = pdbMetaData.getPropId("name")
    }
    val time2 = System.currentTimeMillis()
    println(time1 - time0)
    println(time2 - time1)
  }

  @Test
  def test1(): Unit = {
    pdbMetaData.getPropId("name")
    pdbMetaData.getPropId("age")
    pdbMetaData.getLabelId("label0")
    pdbMetaData.getLabelId("label1")
    pdbMetaData.persist(dbPath)
  }

  // write and read rocksDB takes much time. Better to persist the PDBMetaData in a naive file.
  @Test
  def test2(): Unit = {
    pdbMetaData.init(dbPath)
    Assert.assertEquals(0, pdbMetaData.getLabelId("label0"))
    Assert.assertEquals(1, pdbMetaData.getLabelId("label1"))
    Assert.assertEquals(0, pdbMetaData.getPropId("name"))
    Assert.assertEquals("age", pdbMetaData.getPropName(1))
  }


}
