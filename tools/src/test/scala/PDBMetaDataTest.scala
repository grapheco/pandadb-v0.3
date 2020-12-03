import org.junit.Test

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:08 2020/12/3
 * @Modified By:
 */
class PDBMetaDataTest {
  val pdbMetaData = PDBMetaData
  pdbMetaData.addProp("name")
  val mmap: Map[String, Int] = Map[String, Int]("name" -> 0)

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


}
