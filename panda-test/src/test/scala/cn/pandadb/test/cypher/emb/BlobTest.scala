//package cn.pandadb.test.cypher.emb
//
//import cn.pandadb.kernel.GraphDatabaseBuilder
//import cn.pandadb.kernel.kv.GraphFacade
//import org.apache.commons.io.FileUtils
//import org.junit.{After, Before, Test}
//
//import java.io.File
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 10:27 上午 2021/7/8
// * @Modified By:
// */
//class BlobTest {
//
//  val dbPath = "./testdata/emb"
//  var db: GraphFacade = _
//
//  @Before
//  def init(): Unit ={
//    FileUtils.deleteDirectory(new File(dbPath))
//    FileUtils.forceMkdir(new File(dbPath))
//    db = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath).asInstanceOf[GraphFacade]
//  }
//
//  @After
//  def close(): Unit ={
//    db.close()
//  }
//
//  @Test
//  def testSubproperty() = {
//    val cypherStatement: String = "Create(n:TEST{image:Blob.fromURL(\"http://10.0.90.173:8080/fileServer/two.jpg\")}) Return n.image->faceFeature as result;"
//    val test = db.cypher(cypherStatement)
//    val result = test.records().next()("result")
//    println(result)
//  }
//
//
//}
