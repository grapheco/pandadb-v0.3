package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.rocksdb.{WriteBatch, WriteOptions}

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 21:18 2021/1/15
  * @Modified By:
  */
class SingleRelationFileImporter(file: File, importCmd: ImportCmd, globalArgs: GlobalArgs) extends SingleFileImporter {
  override val csvFile: File = file
  override val cmd: ImportCmd = importCmd
  override val importerFileReader: ImporterFileReader = new ImporterFileReader(csvFile, importCmd.delimeter)
  override val headLine: Array[String] = importerFileReader.getHead.getAsArray
  override val idIndex: Int = headLine.indexWhere(item => item.contains("REL_ID"))
  override val labelIndex: Int = headLine.indexWhere(item => item.contains(":TYPE"))
  override val estLineCount: Long = estLineCount(csvFile)
  override val taskCount: Int = globalArgs.coreNum/4

  service.scheduleWithFixedDelay(importerFileReader.fillQueue, 0, 50, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 1, 1, TimeUnit.SECONDS)

  val fromIdIndex: Int = headLine.indexWhere(item => item.contains(":START_ID"))
  val toIdIndex: Int = headLine.indexWhere(item => item.contains(":END_ID"))

  override val propHeadMap: Map[Int, (Int, String)] = {
    headLine.zipWithIndex.map(item => {
      if(item._2 == idIndex || item._2 == labelIndex || item._2 == fromIdIndex || item._2 == toIdIndex){
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

  val relationDB = globalArgs.relationDB
  val inRelationDB = globalArgs.inrelationDB
  val outRelationDB = globalArgs.outRelationDB
  val relationTypeDB = globalArgs.relationTypeDB

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  val globalCount: AtomicLong = globalArgs.globalRelCount
  val estEdgeCount: Long = estLineCount

  override protected def _importTask(taskId: Int): Boolean = {
    val serializer = RelationSerializer
    var innerCount = 0

    val inBatch = new WriteBatch()
    val outBatch = new WriteBatch()
    val storeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines
      if(batchData.nonEmpty) {
        batchData.foreach(line => {
          innerCount += 1
          val lineArr = line.getAsArray
          val relation = _wrapEdge(lineArr)
          val serializedRel = serializer.serialize(relation)
          storeBatch.put(KeyConverter.toRelationKey(relation.id), serializedRel)
          inBatch.put(KeyConverter.edgeKeyToBytes(relation.to, relation.typeId, relation.from), ByteUtils.longToBytes(relation.id))
          outBatch.put(KeyConverter.edgeKeyToBytes(relation.from, relation.typeId, relation.to), ByteUtils.longToBytes(relation.id))
          labelBatch.put(KeyConverter.toRelationTypeKey(relation.typeId, relation.id), Array.emptyByteArray)

//          if(innerCount % 1000000 == 0) {
//            relationDB.write(writeOptions, storeBatch)
//            inRelationDB.write(writeOptions, inBatch)
//            outRelationDB.write(writeOptions, outBatch)
//            relationTypeDB.write(writeOptions, labelBatch)
//            storeBatch.clear()
//            inBatch.clear()
//            outBatch.clear()
//            labelBatch.clear()
//          }
        })
        relationDB.write(writeOptions, storeBatch)
        inRelationDB.write(writeOptions, inBatch)
        outRelationDB.write(writeOptions, outBatch)
        relationTypeDB.write(writeOptions,labelBatch)
        storeBatch.clear()
        inBatch.clear()
        outBatch.clear()
        labelBatch.clear()
        globalCount.addAndGet(batchData.length)
      }
    }

    relationDB.flush()
    inRelationDB.flush()
    outRelationDB.flush()
    relationTypeDB.flush()
    true
  }

  private def _wrapEdge(lineArr: Array[String]): StoredRelationWithProperty = {
    val relId: Long = lineArr(idIndex).toLong
    val fromId: Long = lineArr(fromIdIndex).toLong
    val toId: Long = lineArr(toIdIndex).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(labelIndex))
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propHeadMap)

    new StoredRelationWithProperty(relId, fromId, toId, edgeType, propMap)
  }


}
