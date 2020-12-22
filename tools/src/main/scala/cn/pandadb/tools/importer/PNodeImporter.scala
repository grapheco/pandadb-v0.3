package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.node.{NodeLabelStore, NodeStore}
import cn.pandadb.kernel.kv.{KeyHandler, RocksDBStorage}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.{BaseSerializer, NodeSerializer}
import org.rocksdb.{WriteBatch, WriteOptions}

import scala.io.Source

object NewNodeTest {
  /*
  100w: 7s
  1yi: 12 min
   */
  def main(args: Array[String]): Unit = {
    val dbPath =  "D:\\PandaDB-tmp\\bench\\newNodeStore"
    val nodeFile = new File("D:\\PandaDB-tmp\\bench\\csv\\nodes_output.csv")
    val nodeHeadFile = new File("D:\\PandaDB-tmp\\bench\\csv\\nodeHeadFile.csv")
    val importer = new PNodeImporter(dbPath, nodeFile, nodeHeadFile)
    importer.importNodes()
  }
}
// TODO 1.label String->Int 2.propName String->Int
class PNodeImporter(dbPath: String, nodeFile: File, nodeHeadFile: File) {

  val NONE_LABEL_ID:Int = -1
  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setNodeHead()

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeStore = new NodeStore(nodeDB)
  private val nodeLabelDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabel")
  private val nodeLabelStore = new NodeLabelStore(nodeLabelDB)

  def importNodes(): Unit = {
    val writeOptions: WriteOptions = new WriteOptions()
    writeOptions.setDisableWAL(true)
    writeOptions.setIgnoreMissingColumnFamilies(true)
    writeOptions.setSync(false)


    val iter = Source.fromFile(nodeFile).getLines()
    var i = 0
    val batchNode: WriteBatch = new WriteBatch()
    val batchLabel: WriteBatch = new WriteBatch()

    var sstime = System.currentTimeMillis()
    while (iter.hasNext) {
      if (i % 10000000 == 0) {
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i / 10000000}% nodes imported. $time1")
      }
      i += 1;

      val tempNode = _wrapNode(iter.next().replace("\n", "").split(","))

      tempNode.labelIds.foreach{labelId =>
        batchNode.put(KeyHandler.nodeKeyToBytes(labelId, tempNode.id), NodeSerializer.serialize(tempNode))
        batchLabel.put(KeyHandler.nodeLabelToBytes(tempNode.id, labelId), Array.emptyByteArray)
      }
      if (tempNode.labelIds.isEmpty) {
        batchNode.put(KeyHandler.nodeKeyToBytes(NONE_LABEL_ID, tempNode.id), NodeSerializer.serialize(tempNode))
        batchLabel.put(KeyHandler.nodeLabelToBytes(tempNode.id, NONE_LABEL_ID), Array.emptyByteArray)
      }

      if (i % 100000 == 0) {
        nodeDB.write(writeOptions, batchNode)
        batchNode.clear()
        println(s"coming~~~10w :${System.currentTimeMillis() - sstime} ms")
        sstime = System.currentTimeMillis()
      }
    }
  }

  private def _setNodeHead(): Map[String, String] = {
    var hMap: Map[String, String] = Map[String, String]()
    val headArr = Source.fromFile(nodeHeadFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[String](headArr.length - 2)
    // headArr(0) is :ID, headArr(1) is :LABELS
    for (i <- 2 until headArr.length) {
      val fieldArr = headArr(i).split(":")
      val propName: String = fieldArr(0)
      propSortArr(i - 2) = propName
      val propType: String = fieldArr(1).toLowerCase()
      hMap += (propName -> propType)
    }
    hMap
  }

  private def _wrapNode(lineArr: Array[String]): StoredNodeWithProperty = {
    val id = lineArr(0).toLong
    //  TODO：modify the labels import mechanism, enable real array
    val labels: Array[Int] = Array(PDBMetaData.getLabelId(lineArr(1)))
    var propMap: Map[Int, Any] = Map[Int, Any]()
    for (i <- 2 to lineArr.length - 1) {
      val propName = propSortArr(i - 2)
      val propValue: Any = {
        headMap(propName) match {
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += ((i - 2) -> propValue)
    }
    new StoredNodeWithProperty(id, labels, propMap)
  }
}
