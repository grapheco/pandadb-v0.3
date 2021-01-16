package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.util.serializer.NodeSerializer
import org.rocksdb.{WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 10:20 2021/1/15
 * @Modified By:
 */
class SingleNodeFileImporter(file: File, importCmd: ImportCmd, globalArgs: GlobalArgs) extends SingleFileImporter {
  override val csvFile: File = file
  override val cmd: ImportCmd = importCmd
  override val importerFileReader: ImporterFileReader = new ImporterFileReader(file, importCmd.delimeter)
  override val headLine: Array[String] = importerFileReader.getHead.getAsArray
  override val idIndex: Int = headLine.indexWhere(item => item.contains(":ID"))
  override val labelIndex: Int = headLine.indexWhere(item => item.contains(":LABEL"))
  override val estLineCount: Long = estLineCount(csvFile)
  override val taskCount: Int = globalArgs.coreNum/4
  override val propHeadMap: Map[Int, (Int, String)] = {
    headLine.zipWithIndex.map(item => {
      if(item._2 == idIndex || item._2 == labelIndex){
        (-1, (-1, ""))
      } else {
        val pair = item._1.split(":")
        val propId = PDBMetaData.getPropId(pair(0))
        val propType = {
          if (pair.length == 2) pair(1).toLowerCase()
          else "string"
        }
        (item._2, (propId, propType))
      }
    }).toMap.filter(item => item._1 > -1)
  }

  service.scheduleWithFixedDelay(importerFileReader.fillQueue, 0, 50, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 1, 1, TimeUnit.SECONDS)

  val nodeDB = globalArgs.nodeDB
  val nodeLabelDB = globalArgs.nodeLabelDB
  val globalCount = globalArgs.globalNodeCount
  val estNodeCount = globalArgs.estNodeCount
  val NONE_LABEL_ID = -1

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

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
//        if (innerCount % 1000000 == 0) {
//          nodeDB.write(writeOptions, nodeBatch)
//          nodeLabelDB.write(writeOptions, labelBatch)
//          nodeBatch.clear()
//          labelBatch.clear()
//        }
      })
      nodeDB.write(writeOptions, nodeBatch)
      nodeLabelDB.write(writeOptions, labelBatch)
      nodeBatch.clear()
      labelBatch.clear()
      globalCount.addAndGet(batchData.length)
    }

    nodeDB.flush()
    nodeLabelDB.flush()
    true
  }

  private def _wrapNode(lineArr: Array[String]): (Long, Array[Int], Map[Int, Any]) = {
    val id = lineArr(idIndex).toLong
    val labels: Array[String] = lineArr(labelIndex).split(importCmd.arrayDelimeter)
    val labelIds: Array[Int] = labels.map(label => PDBMetaData.getLabelId(label))
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propHeadMap)
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
