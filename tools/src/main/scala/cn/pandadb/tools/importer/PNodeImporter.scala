package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{FlushOptions, RocksDB, WriteBatch, WriteOptions}

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

class SingleFileNodeImporter(nodeFile: File, nodeDB: RocksDB, nodeLabelDB: RocksDB) {


}
class PNodeImporter(dbPath: String, nodeHeadFile: File, nodeFile: File) extends Importer with Logging {
//class PNodeImporter(importCmd: ImportCmd) extends Importer with Logging {

//  val dbPath = importCmd.exportDBPath

  val NONE_LABEL_ID: Int = -1
  override protected var propSortArr: Array[Int] = null //Array(propId), record the sort of propId in head file
  override val headMap: Map[Int, String] = _setNodeHead()  // map(propId -> type)
  override val importerFileReader = new ImporterFileReader(nodeFile, ",",500000)

  override val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
//  override val service: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
  service.scheduleWithFixedDelay(importerFileReader.fillQueue, 0, 50, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 1, 1, TimeUnit.SECONDS)

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes", useForImporter = true)
  private val nodeLabelDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabel", useForImporter = true)

  val estNodeCount: Long = estLineCount(nodeFile)
  var globalCount: AtomicLong = new AtomicLong(0)

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  def importNodes(): Unit = {
    importData()
    nodeDB.close()
    nodeLabelDB.close()
    logger.info(s"$globalCount nodes imported.")
  }

  override protected def _importTask(taskId: Int): Boolean = {
    val serializer = NodeSerializer
    var innerCount = 0
    val nodeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines
      batchData.foreach(line => {
        innerCount += 1
//        val lineArr = line.replace("\n", "").split(",")
        val lineArr = line.getAsArray
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
      // forbid to access file reader at same time
      Thread.sleep(10*taskId)
    }

    nodeDB.flush()
    nodeLabelDB.flush()
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
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propSortArr, 2)
    (id, labelIds, propMap)
  }

  private def _getNodeKeys(id: Long, labelIds: Array[Int]): Array[(Array[Byte], Array[Byte])] = {
    if(labelIds.isEmpty) {
      val nodeKey = KeyConverter.toNodeKey(NONE_LABEL_ID, id)
      val labelKey = KeyConverter.toNodeLabelKey(id, NONE_LABEL_ID)
      Array((nodeKey, labelKey))
    } else {
      labelIds.map(label => {
        val nodeKey = KeyConverter.toNodeKey(label, id)
        val labelKey = KeyConverter.toNodeLabelKey(id, label)
        (nodeKey, labelKey)
      })
    }
  }
}
