//import org.junit.{Assert, Test}
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 13:58 2020/12/8
// * @Modified By:
// */
//
////# target neo4j node format: id(str), label(str), id_p(long),idStr(string),flag(bool)
////# target neo4j edge format: fromId(long), type(str), toId(long), fromIn_p(long), toId_p(str), category(int)
//class PerfTest extends PerfTestBase {
//
//
//
//
//}
//
//case class NodeArgs(id: Long, labels: Array[Int],  id_p: Long,  idStr: String, flag: Boolean)
//class PerfTestBase {
//
//  def randId(max: Int): Long = {
//    scala.util.Random.nextInt(max).toLong
//  }
//
//  def getNodeArgs(nodeId: Long): NodeArgs = {
//    val labels: Array[Int] = Array((nodeId % 10).toInt)
//    val id_p: Long = nodeId
//    val idStr: String = {
//      val strBuilder: StringBuilder = new StringBuilder
//      // 48 for 0, 97 for a
//      nodeId.toString.foreach(char => strBuilder.append(char match {
//        case '0' => 'a'
//        case '1' => 'b'
//        case '2' => 'c'
//        case '3' => 'd'
//        case '4' => 'e'
//        case '5' => 'f'
//        case '6' => 'g'
//        case '7' => 'h'
//        case '8' => 'i'
//        case '9' => 'j'
//      }))
//      strBuilder.toString()
//    }
//    val flag: Boolean = nodeId%2 match {
//      case 0 => false
//      case 1 => true
//    }
//    NodeArgs(nodeId, labels, id_p, idStr, flag)
//  }
//
//  @Test
//  def testNodeArgs(): Unit ={
//    val expectedNodeArgs = NodeArgs(12345.toLong, Array(5), 12345, "bcdef", true)
//    val actualNodeArgs = getNodeArgs(12345)
//    Assert.assertEquals(expectedNodeArgs.id, actualNodeArgs.id)
//    Assert.assertArrayEquals(expectedNodeArgs.labels, actualNodeArgs.labels)
//    Assert.assertEquals(expectedNodeArgs.id_p, actualNodeArgs.id_p)
//    Assert.assertEquals(expectedNodeArgs.idStr, actualNodeArgs.idStr)
//    Assert.assertEquals(expectedNodeArgs.flag, actualNodeArgs.flag)
//  }
//
//}
//
