package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{KeyHandler, RocksDBStorage}
import cn.pandadb.kernel.store.StoredNodeWithProperty
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{FlushOptions, WriteBatch, WriteOptions}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 17:22 2020/12/3
 * @Modified By:
 */

/**
 *
headMap(propName1 -> type, propName2 -> type ...)
 */
// protocol: :ID :LABELS propName1:type1 proName2:type2
class PNodeImporter(dbPath: String, nodeFile: File, nodeHeadFile: File) extends Importer with Logging {

  val NONE_LABEL_ID: Int = -1
  private var propSortArr: Array[Int] = null //Array(propId), record the sort of propId in head file
  private val headMap: Map[Int, String] = _setNodeHead()  // map(propId -> type)
  val importerFileReader = new ImporterFileReader(nodeFile, 1000000)

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeLabelDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabel")

  val estNodeCount: Long = estLineCount(nodeFile)
  var globalCount: Int = 0

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  def importNodes(): Unit = {
    val f0 = Future{_importTask(0)}
    val f1 = Future{_importTask(1)}
    val f2 = Future{_importTask(2)}
    val f3 = Future{_importTask(3)}
    Await.result(f0, Duration.Inf)
    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)
    Await.result(f3, Duration.Inf)
    logger.info(s"$globalCount nodes imported.")
  }

  private def _importTask(taskId: Int): Boolean = {
    val serializer: NodeSerializer = new NodeSerializer()
    var innerCount = 0
    val nodeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val array = importerFileReader.getLines()
      array.foreach(line => {
        this.synchronized(globalCount += 1)
        innerCount += 1
        if (globalCount % 10000000 == 0) {
          val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
          logger.info(s"${globalCount / 10000000}kw of $estNodeCount(est) nodes imported. $time1, thread$taskId")
        }
        val lineArr = line.replace("\n", "").split(",")
        val node = _wrapNode(lineArr)
        val keys: Array[(Array[Byte], Array[Byte])] = _getNodeKeys(node)
        val serializedNodeValue = serializer.serialize(node)
        keys.foreach(pair =>{
          nodeBatch.put(pair._1, serializedNodeValue)
          labelBatch.put(pair._2, Array.emptyByteArray)
        })
        if (innerCount % 1000000 == 0) {
          nodeDB.write(writeOptions, nodeBatch)
          nodeLabelDB.write(writeOptions, labelBatch)
          nodeBatch.clear()
          labelBatch.clear()
        }
      })
      nodeDB.write(writeOptions, nodeBatch)
      nodeLabelDB.write(writeOptions, labelBatch)
      nodeBatch.clear()
      labelBatch.clear()
    }
    val flushOptions = new FlushOptions
    nodeDB.flush(flushOptions)
    nodeLabelDB.flush(flushOptions)
    logger.info(s"$innerCount, $taskId")
    true
  }

  private def _setNodeHead(): Map[Int, String] = {
    var hMap: Map[Int, String] = Map[Int, String]()
    val headArr = Source.fromFile(nodeHeadFile).getLines().next().replace("\n", "").split(",")
    propSortArr = new Array[Int](headArr.length - 2)
    // headArr(0) is :ID, headArr(1) is :LABELS
    for (i <- 2 until headArr.length) {
      val fieldArr = headArr(i).split(":")
      val propId: Int = PDBMetaData.getPropId(fieldArr(0))
      propSortArr(i - 2) = propId
      val propType: String = fieldArr(1).toLowerCase()
      hMap += (propId -> propType)
    }
    hMap
  }

  private def _wrapNode(lineArr: Array[String]): StoredNodeWithProperty = {
    val id = lineArr(0).toLong
    val labels: Array[String] = lineArr(1).split(";")
    val labelIds: Array[Int] = labels.map(label => PDBMetaData.getLabelId(label))
    var propMap: Map[Int, Any] = Map[Int, Any]()
    for (i <- 2 to lineArr.length - 1) {
      val propId: Int = propSortArr(i - 2)
      val propValue: Any = {
        headMap(propId) match {
          case "float" => lineArr(i).toFloat
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += (propId -> propValue)
    }
    new StoredNodeWithProperty(id, labelIds, propMap)
  }

  private def _getNodeKeys(node: StoredNodeWithProperty): Array[(Array[Byte], Array[Byte])] = {
    if(node.labelIds.isEmpty) {
      val nodeKey = KeyHandler.nodeKeyToBytes(NONE_LABEL_ID, node.id)
      val labelKey = KeyHandler.nodeLabelToBytes(node.id, NONE_LABEL_ID)
      Array((nodeKey, labelKey))
    } else {
      node.labelIds.map(label => {
        val nodeKey = KeyHandler.nodeKeyToBytes(label, node.id)
        val labelKey = KeyHandler.nodeLabelToBytes(node.id, label)
        (nodeKey, labelKey)
      })
    }
  }
}
