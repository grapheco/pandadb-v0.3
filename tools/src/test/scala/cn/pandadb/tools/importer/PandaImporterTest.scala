package cn.pandadb.tools.importer

import java.io.File

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{StoredNodeWithProperty, StoredRelationWithProperty}
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}
import org.apache.logging.log4j.scala.Logging

import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 14:48 2020/12/28
 * @Modified By:
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class PandaImporterTest extends Logging {
  val dbPath: String = "C://PandaDB/base_1kw"
  val nodeHeadFile: File = new File("D:\\GitSpace\\ScalaUtils\\output//nodeHead.csv")
  val nodeFile: File = new File("D://GitSpace//ScalaUtils//output//nodes1kw.csv")
  val relationHeadFile: File = new File("D:\\GitSpace\\ScalaUtils\\output//relationHead.csv")
  val relationFile: File = new File("D://GitSpace//ScalaUtils//output//edges1kw.csv")

  // nodePropHead: id_p:Long	idStr:String	flag:Boolean	name:String
  // relPropHead: fromID:long, toID:String, weight:int
  val nodePropHead: Array[(String, String)] = Array(("id_p", "long"), ("idStr", "string"), ("flag", "boolean"), ("name", "string"))
  val relPropHead: Array[(String, String)] = Array(("fromID", "long"), ("toID", "string"), ("weight", "int"))

  // import data
  @Test
  def test1() = {
    val args: Array[String] = Array(dbPath, nodeHeadFile.getCanonicalPath, nodeFile.getCanonicalPath, relationHeadFile.getCanonicalPath, relationFile.getCanonicalPath)
    PandaImporter.main(args)
  }

  //node correct check
  @Test
  def test2() = {
    PDBMetaData.init(dbPath)
    val iter: Iterator[String] = Source.fromFile(nodeFile).getLines()
    val nodeAPI = new NodeStoreAPI(dbPath)
    var count: Int = 0
    while (iter.hasNext) {
      val nodeFromFile = getNodeByLine(iter.next(), nodePropHead)
      val nodeFromDB = nodeAPI.getNodeById(nodeFromFile.id)
      Assert.assertEquals(nodeFromFile.id, nodeFromDB.get.id)
      Assert.assertArrayEquals(nodeFromFile.labelIds, nodeFromDB.get.labelIds)
      Assert.assertEquals(nodeFromFile.properties, nodeFromDB.get.properties)
      count += 1
      if(count % 100000 == 0) logger.info(s"${count/100000} * 10W nodes checked.")
    }
  }

  //relation correct check
  @Test
  def test3() = {
    PDBMetaData.init(dbPath)
    val a = PDBMetaData
    val iter = Source.fromFile(relationFile).getLines()
    val relationAPI = new RelationStoreAPI(dbPath)
    var count: Int = 0
    while (iter.hasNext) {
      val relFromFile = getRelationByLine(iter.next(), relPropHead)
      val relFromDB = relationAPI.getRelationById(relFromFile.id)
      Assert.assertEquals(relFromFile, relFromDB)
      count += 1
      if(count % 100000 == 0) logger.info(s"${count/100000} * 10W rels checked.")
    }

  }

  def getNodeByLine(line: String, propHead: Array[(String, String)]): StoredNodeWithProperty = {
    val lineArr: Array[String] = line.split(",")
    val id = lineArr(0).toLong
    val labels: Array[Int] = lineArr(1).split(";").map(label => PDBMetaData.getLabelId(label))
    val propMap: Map[Int, Any] = Map(PDBMetaData.getPropId("id_p") -> lineArr(2).toLong,
      PDBMetaData.getPropId("idStr") -> lineArr(3),
      PDBMetaData.getPropId("flag") -> lineArr(4).toBoolean,
      PDBMetaData.getPropId("name") -> lineArr(5))
    new StoredNodeWithProperty(id, labels, propMap)
  }

  def getRelationByLine(line: String, propHead: Array[(String, String)]): StoredRelationWithProperty = {
    val lineArr: Array[String] = line.split(",")
    val relationId = lineArr(0).toLong
    //:REL_ID,:FROMID,:TOID,:type,fromID:long,toID:String,weight:int
    val fromId = lineArr(1).toLong
    val toId = lineArr(2).toLong
    val typeId: Int = PDBMetaData.getTypeId(lineArr(3))
    val propMap: Map[Int, Any] = Map(PDBMetaData.getPropId("fromID") -> lineArr(4).toLong, PDBMetaData.getPropId("toID") -> lineArr(5),
      PDBMetaData.getPropId("weight") -> lineArr(6).toInt)
    new StoredRelationWithProperty(relationId, fromId, toId, typeId, propMap)
  }

}
