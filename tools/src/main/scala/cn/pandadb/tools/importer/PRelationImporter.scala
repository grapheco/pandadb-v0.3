package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{FlushOptions, WriteBatch, WriteOptions}

import scala.util.Random

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:01 2020/12/4
 * @Modified By:
 */
// TODO 1.relationLabel 2.relationType String->Int

/**
 * protocol: :relId(long), :fromId(long), :toId(long), :edgetype(string), propName1:type, ...
 */
class PRelationImporter(dbPath: String, headFile: File, edgeFile: File) extends Importer with Logging{

  override val importerFileReader = new ImporterFileReader(edgeFile, 500000)
  override var propSortArr: Array[Int] = null
  override val headMap: Map[Int, String] = _setEdgeHead()

  override val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleWithFixedDelay(importerFileReader.fillQueue, 0, 50, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 1, 1, TimeUnit.SECONDS)

  val relationDB = RocksDBStorage.getDB(s"${dbPath}/rels", useForImporter = true)
  val inRelationDB = RocksDBStorage.getDB(s"${dbPath}/inEdge", useForImporter = true)
  val outRelationDB = RocksDBStorage.getDB(s"${dbPath}/outEdge", useForImporter = true)
  val relationTypeDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex", useForImporter = true)

  var globalCount: AtomicLong = new AtomicLong(0)
  val estEdgeCount: Long = estLineCount(edgeFile)

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  def importRelations(): Unit ={
    importData()
    relationDB.close()
    inRelationDB.close()
    outRelationDB.close()
    relationTypeDB.close()
    logger.info(s"$globalCount relations imported.")
  }

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
          val lineArr = line.replace("\n", "").split(",")
          val relation = _wrapEdge(lineArr)
          val serializedRel = serializer.serialize(relation)
          storeBatch.put(KeyConverter.toRelationKey(relation.id), serializedRel)
          inBatch.put(KeyConverter.edgeKeyToBytes(relation.to, relation.typeId, relation.from), ByteUtils.longToBytes(relation.id))
          outBatch.put(KeyConverter.edgeKeyToBytes(relation.from, relation.typeId, relation.to), ByteUtils.longToBytes(relation.id))
          labelBatch.put(KeyConverter.toRelationTypeKey(relation.typeId, relation.id), Array.emptyByteArray)

          if(innerCount % 1000000 == 0) {
            relationDB.write(writeOptions, storeBatch)
            inRelationDB.write(writeOptions, inBatch)
            outRelationDB.write(writeOptions, outBatch)
            relationTypeDB.write(writeOptions, labelBatch)
            storeBatch.clear()
            inBatch.clear()
            outBatch.clear()
            labelBatch.clear()
          }
        })
        relationDB.write(writeOptions, storeBatch)
        inRelationDB.write(writeOptions, inBatch)
        outRelationDB.write(writeOptions, outBatch)
        relationTypeDB.write(writeOptions,labelBatch)
        storeBatch.clear()
        inBatch.clear()
        outBatch.clear()
        labelBatch.clear()
        if(globalCount.addAndGet(batchData.length) % 10000000 == 0){
          val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
          logger.info(s"${globalCount.get() / 10000000}kw of $estEdgeCount(est) edges imported. $time1")
        }
      }
      // forbid to access file reader at same time
      Thread.sleep(10*taskId)
    }


    relationDB.flush()
    inRelationDB.flush()
    outRelationDB.flush()
    relationTypeDB.flush()
    logger.info(s"$innerCount, $taskId")
    true
  }

  private def _setEdgeHead(): Map[Int, String] = {
    _setHead(4, headFile)
  }

  private def _wrapEdge(lineArr: Array[String]): StoredRelationWithProperty = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propSortArr, 4)

    new StoredRelationWithProperty(relId, fromId, toId, edgeType, propMap)
  }
}
