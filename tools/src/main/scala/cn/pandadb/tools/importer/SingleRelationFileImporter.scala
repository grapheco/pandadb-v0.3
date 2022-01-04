package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{ByteUtils}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer

import scala.collection.convert.ImplicitConversions._
import java.io.File
import java.util.concurrent.ConcurrentHashMap

import cn.pandadb.kernel.distribute.DistributedKeyConverter
import cn.pandadb.kernel.distribute.relationship.RelationDirection

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
  override val taskCount: Int = globalArgs.coreNum / 8

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
  val inRelationDB = globalArgs.inRelationDB
  val outRelationDB = globalArgs.outRelationDB
  val typeRelationDB = globalArgs.relationTypeDB

  val innerFileRelCountByType: ConcurrentHashMap[Int, Long] = new ConcurrentHashMap[Int, Long]()
  val estEdgeCount: Long = estLineCount

  override protected def _importTask(taskId: Int): Boolean = {
    val innerTaskRelCountByType: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]()
    val serializer = RelationSerializer

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines
      if(batchData.nonEmpty) {
        val processedData = batchData.map(line => {
          val lineArr = line.getAsArray
          val relation = _wrapEdge(lineArr)
          val serializedRel = serializer.serialize(relation)
          _countMapAdd(innerTaskRelCountByType, relation.typeId, 1L)

          (
            (DistributedKeyConverter.toRelationKey(relation.id), serializedRel),
            (DistributedKeyConverter.edgeKeyToBytes(relation.to, relation.typeId, relation.from, RelationDirection.IN), ByteUtils.longToBytes(relation.id)),
            (DistributedKeyConverter.edgeKeyToBytes(relation.from, relation.typeId, relation.to, RelationDirection.OUT), ByteUtils.longToBytes(relation.id)),
            (DistributedKeyConverter.toRelationTypeKey(relation.typeId, relation.id), Array.emptyByteArray)
          )
        })

        val relationBatch = processedData.map(f => f._1)
        val inBatch = processedData.map(f => f._2)
        val outBatch = processedData.map(f => f._3)
        val typeBatch = processedData.map(f => f._4)
        val f1: Future[Unit] = Future{relationBatch.grouped(100000).foreach(batch => batch.grouped(10000).toList.par.foreach(_batch => relationDB.batchPut(_batch)))}
        val f2: Future[Unit] = Future{inBatch.grouped(100000).foreach(batch => batch.grouped(10000).toList.par.foreach(_batch => inRelationDB.batchPut(_batch)))}
        val f3: Future[Unit] = Future{outBatch.grouped(100000).foreach(batch => batch.grouped(10000).toList.par.foreach(_batch => outRelationDB.batchPut(_batch)))}
        val f4: Future[Unit] = Future{typeBatch.grouped(100000).foreach(batch => batch.grouped(10000).toList.par.foreach(_batch => typeRelationDB.batchPut(_batch)))}
        Await.result(f1, Duration.Inf)
        Await.result(f2, Duration.Inf)
        Await.result(f3, Duration.Inf)
        Await.result(f4, Duration.Inf)

        globalArgs.importerStatics.relCountAddBy(batchData.length)
        globalArgs.importerStatics.relPropCountAddBy(batchData.length*propHeadMap.size)
      }
    }

    innerTaskRelCountByType.foreach(kv => _countMapAdd(innerFileRelCountByType, kv._1, kv._2))

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

  override protected def _commitInnerFileStatToGlobal(): Boolean = {
    innerFileRelCountByType.foreach(kv => globalArgs.importerStatics.relTypeCountAdd(kv._1, kv._2))
    true
  }

}
