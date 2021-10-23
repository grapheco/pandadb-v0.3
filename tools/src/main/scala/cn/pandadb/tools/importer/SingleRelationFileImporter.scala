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
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

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
  override val idIndex: Int = {
    val columnId: Int = headLine.indexWhere(item => item.contains("REL_ID"))
    if (columnId == -1) throw new Exception("No :REL_ID column.")
    columnId
  }
  override val labelIndex: Int = {
    val columnId: Int = headLine.indexWhere(item => item.contains(":TYPE"))
    if (columnId == -1) throw new Exception("No :TYPE column.")
    columnId
  }
  override val estLineCount: Long = estLineCount(csvFile)
  override val taskCount: Int = globalArgs.coreNum/4

  val fromIdIndex: Int = headLine.indexWhere(item => item.contains(":START_ID"))
  val toIdIndex: Int = headLine.indexWhere(item => item.contains(":END_ID"))

  override val propHeadMap: Map[Int, (Int, String)] = {
    headLine.zipWithIndex.map(item => {
      if(item._2 == idIndex || item._2 == labelIndex || item._2 == fromIdIndex || item._2 == toIdIndex){
        if(item._1.split(":")(0).length == 0 || item._1.split(":")(0) == "REL_ID") {
          (-1, (-1, ""))
        } else {
          val pair = item._1.split(":")
          if(pair(0)=="") throw new Exception(s"Missed property name in column ${item._2}.")
          val propId = PDBMetaData.getPropId(pair(0))
          val propType = "string"
          (item._2, (propId, propType))
        }
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

  val innerFileRelCount: AtomicLong = new AtomicLong()
  val innerFileRelCountByType: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]()
//  val globalCount: AtomicLong = globalArgs.globalRelCount
//  val globalPropCount: AtomicLong = globalArgs.globalRelPropCount
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

        })
        val f1: Future[Unit] = Future{relationDB.write(writeOptions, storeBatch)}
        val f2: Future[Unit] = Future{inRelationDB.write(writeOptions, inBatch)}
        val f3: Future[Unit] = Future{outRelationDB.write(writeOptions, outBatch)}
        val f4: Future[Unit] = Future{relationTypeDB.write(writeOptions,labelBatch)}
        Await.result(f1, Duration.Inf)
        Await.result(f2, Duration.Inf)
        Await.result(f3, Duration.Inf)
        Await.result(f4, Duration.Inf)

        storeBatch.clear()
        inBatch.clear()
        outBatch.clear()
        labelBatch.clear()
        globalArgs.importerStatics.relCountAddBy(batchData.length)
        globalArgs.importerStatics.relPropCountAddBy(batchData.length*propHeadMap.size)
        innerFileRelCountByType.foreach(kv => globalArgs.importerStatics.relTypeCountAdd(kv._1, kv._2))
//        globalCount.addAndGet(batchData.length)
//        globalArgs.statistics.increaseRelationCount(batchData.length)
//        globalPropCount.addAndGet(batchData.length*propHeadMap.size)
      }
    }

    val f1: Future[Unit] = Future{relationDB.flush()}
    val f2: Future[Unit] = Future{inRelationDB.flush()}
    val f3: Future[Unit] = Future{outRelationDB.flush()}
    val f4: Future[Unit] = Future{relationTypeDB.flush()}
    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)
    Await.result(f3, Duration.Inf)
    Await.result(f4, Duration.Inf)

    true
  }

  private def _wrapEdge(lineArr: Array[String]): StoredRelationWithProperty = {
    val relId: Long = lineArr(idIndex).toLong
    val fromId: Long = lineArr(fromIdIndex).toLong
    val toId: Long = lineArr(toIdIndex).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(labelIndex))
    globalArgs.statistics.increaseRelationTypeCount(edgeType, 1)
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propHeadMap)

    _countMapAdd(innerFileRelCountByType, edgeType, 1)
    new StoredRelationWithProperty(relId, fromId, toId, edgeType, propMap)
  }

}
