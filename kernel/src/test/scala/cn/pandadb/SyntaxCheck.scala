package cn.pandadb

import cn.pandadb.kernel.kv.{GraphFacadeWithPPD, NodeLabelStore, PropertyNameStore, RelationLabelStore, RocksDBGraphAPI, TokenStore}
import cn.pandadb.kernel.store.FileBasedIdGen
import org.apache.commons.io.FileUtils
import org.junit.{After, Before, Test}
import org.apache.commons.csv.CSVFormat
import java.io.FileReader

import java.io.File

class SyntaxCheck {
  var nodeLabelStore: TokenStore = _
  var relLabelStore: TokenStore = _
  var propNameStore: TokenStore = _
  var graphStore: RocksDBGraphAPI = _
  var graphFacade: GraphFacadeWithPPD = _

  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()
    graphStore = new RocksDBGraphAPI("./testdata/output/rocksdb")
    nodeLabelStore = new NodeLabelStore(graphStore.getRocksDB)
    relLabelStore = new RelationLabelStore(graphStore.getRocksDB)
    propNameStore = new PropertyNameStore(graphStore.getRocksDB)
    graphFacade = new GraphFacadeWithPPD( nodeLabelStore, relLabelStore, propNameStore,
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      graphStore,
      {}
    )
  }

  @Test
  def testSyntaxQuery(): Unit = {
    val statements =  CSVFormat.EXCEL.withFirstRecordAsHeader().parse(
      new FileReader("./testResource/cypher-syntax-checklist.csv")
    ).getRecords()
    val itr = statements.iterator()
    var errorStatements = new collection.mutable.ArrayBuffer[(String, String)]()
    while(itr.hasNext()){
      val s = itr.next()
      val id = s.get("ID")
      val q = s.get("TestQuery")
      val t = s.get("Type")
      println(s"check TestQuery ${id}: ${q}:")
      try {
        graphFacade.cypher(q)
        println("Passed")
      } catch {
        case e: Exception =>  errorStatements.append((s"id: ${id}, type: ${t}, query: ${q}", s"error: ${e}"))
      }
    }
    println("=========ERROR Massages=============")
    errorStatements.foreach(e => {
      println(e)
      println("-----------------------------------")
    })
    println(s"Summary: error ratio is ${errorStatements.size}/${statements.size()}")
  }

  @Test
  def testTypeQuery(): Unit = {
    val statements = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(
      new FileReader("./testResource/cypher-datatype-checklist.csv")
    ).getRecords()
    val itr = statements.iterator()
    var errorStatements = new collection.mutable.ArrayBuffer[(String, String)]()
    while(itr.hasNext()){
      val s = itr.next()
      val id = s.get("ID")
      val q = s.get("TestQuery")
      val t = s.get("Type")
      println(s"check TestQuery ${id}: ${q}:")
      try {
        graphFacade.cypher(q)
        println("Passed")
      } catch {
        case e: Exception =>  errorStatements.append((s"id: ${id}, type: ${t}, query: ${q}", s"error: ${e}"))
      }
    }
    println("=========ERROR Massages=============")
    errorStatements.foreach(e => {
      println(e)
      println("-----------------------------------")
    })
    println(s"Summary: error ratio is ${errorStatements.size}/${statements.size()}")
  }

  @After
  def close(): Unit ={
    graphFacade.close()
  }

}
