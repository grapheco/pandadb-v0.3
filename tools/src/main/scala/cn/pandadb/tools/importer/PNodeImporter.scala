package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{KeyHandler, RocksDBStorage}
import cn.pandadb.kernel.util.serializer.NodeSerializer
import cn.pandadb.tools.importer.IFReader.reader
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
  override protected var propSortArr: Array[Int] = null //Array(propId), record the sort of propId in head file
  override val headMap: Map[Int, String] = _setNodeHead()  // map(propId -> type)
  override val importerFileReader = new ImporterFileReader(nodeFile, 500000)

  override val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  service.scheduleAtFixedRate(importerFileReader.fillQueue, 0, 100, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 0, 1, TimeUnit.SECONDS)

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeLabelDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabel")

  val estNodeCount: Long = estLineCount(nodeFile)
  var globalCount: AtomicLong = new AtomicLong(0)

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  def importNodes(): Unit = {
    importData()
    logger.info(s"$globalCount nodes imported.")
  }

  override protected def _importTask(taskId: Int): Boolean = {
    val serializer: NodeSerializer = new NodeSerializer()
    var innerCount = 0
    val nodeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines()
      batchData.foreach(line => {
        innerCount += 1
        val lineArr = line.replace("\n", "").split(",")
        val node = _wrapNode(lineArr)
        val keys: Array[(Array[Byte], Array[Byte])] = _getNodeKeys(node._1, node._2)
        val serializedNodeValue = serializer.serialize(node._1, node._2, node._3)
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
      if (globalCount.addAndGet(batchData.length) % 10000000 == 0) {
        val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        logger.info(s"${globalCount.get() / 10000000}kw of $estNodeCount(est) nodes imported. $time, thread$taskId")
      }
    }
    val flushOptions = new FlushOptions
    nodeDB.flush(flushOptions)
    nodeLabelDB.flush(flushOptions)
    logger.info(s"$innerCount, $taskId")
    true
  }

  private def _setNodeHead(): Map[Int, String] = {
    _setHead(2, nodeHeadFile)
  }

  private def _wrapNode(lineArr: Array[String]): (Long, Array[Int], Map[Int, Any]) = {
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
    (id, labelIds, propMap)
  }

  private def _getNodeKeys(id: Long, labelIds: Array[Int]): Array[(Array[Byte], Array[Byte])] = {
    if(labelIds.isEmpty) {
      val nodeKey = KeyHandler.nodeKeyToBytes(NONE_LABEL_ID, id)
      val labelKey = KeyHandler.nodeLabelToBytes(id, NONE_LABEL_ID)
      Array((nodeKey, labelKey))
    } else {
      labelIds.map(label => {
        val nodeKey = KeyHandler.nodeKeyToBytes(label, id)
        val labelKey = KeyHandler.nodeLabelToBytes(id, label)
        (nodeKey, labelKey)
      })
    }
  }
}
