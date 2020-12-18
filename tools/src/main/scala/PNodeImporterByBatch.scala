import java.io.{BufferedInputStream, BufferedReader, DataInputStream, File, FileInputStream, FileReader}
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.kv.{KeyHandler, NodeLabelIndex, NodeStore, NodeValue, NodeValue_tobe_deprecated, RocksDBGraphAPI, RocksDBStorage}
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Test2 {
  /*
  100w: 7s
  1yi: 12 min
   */
  def main(args: Array[String]): Unit = {
    val dbPath = "D:\\data\\rocksdbBatchTest"
    val nodeFile = new File("D:\\data\\nodes_output.csv")
    val nodeHeadFile = new File("D:\\data\\nodeHeadFile.csv")
    val importer = new PNodeImporterByBatch(dbPath, nodeFile, nodeHeadFile)
    importer.importNodes()
  }
}

class PNodeImporterByBatch(dbPath: String, nodeFile: File, nodeHeadFile: File) {
  //  val dbPath = "D:\\data\\rocksdbBatch"

  var propSortArr: Array[String] = null
  val headMap: Map[String, String] = _setNodeHead()

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val labelIndexDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabelIndex")

  def importNodes(): Unit = {
    val writeOptions: WriteOptions = new WriteOptions()
    writeOptions.setDisableWAL(true)
    writeOptions.setIgnoreMissingColumnFamilies(true)
    writeOptions.setSync(false)


    val iter = Source.fromFile(nodeFile).getLines()
    var i = 0
    var batchNode: WriteBatch = new WriteBatch()
    var batchIndex: WriteBatch = new WriteBatch()

    var sstime = System.currentTimeMillis()
    while (iter.hasNext) {
      if (i % 10000000 == 0) {
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        println(s"${i / 10000000}% nodes imported. $time1")
      }
      i += 1;

      val tempNode = _wrapNode(iter.next().replace("\n", "").split(","))

      val keyBytes = KeyHandler.nodeKeyToBytes(tempNode.id)
      batchNode.put(keyBytes, NodeValue_tobe_deprecated.toBytes(tempNode.id, tempNode.labels, tempNode.properties))

      tempNode.labels.foreach(label => {
        val keyBytesIndex = KeyHandler.nodeLabelIndexKeyToBytes(label, tempNode.id)
        batchIndex.put(keyBytesIndex, Array[Byte]())
      })

      if (i % 100000 == 0) {
        nodeDB.write(writeOptions, batchNode)
        labelIndexDB.write(writeOptions, batchIndex)
        batchNode.clear()
        batchIndex.clear()
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
    for (i <- 2 to headArr.length - 1) {
      val fieldArr = headArr(i).split(":")
      val propName: String = fieldArr(0)
      propSortArr(i - 2) = propName
      val propType: String = fieldArr(1).toLowerCase()
      hMap += (propName -> propType)
    }
    hMap
  }

  private def _wrapNode(lineArr: Array[String]): TempNode = {
    val id = lineArr(0).toLong
    //  TODO：modify the labels import mechanism, enable real array
    val labels: Array[Int] = Array(PDBMetaData.getLabelId(lineArr(1)))
    var propMap: Map[String, Any] = Map[String, Any]()
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
      propMap += (propName -> propValue)
    }
    TempNode(id, labels, propMap)
  }
}
