package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBStorage}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{FlushOptions, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:01 2020/12/4
 * @Modified By:
 */
// TODO 1.relationLabel 2.relationType String->Int

/**
 *
 * protocol: :relId(long), :fromId(long), :toId(long), :edgetype(string), category(Int), propName1:type, ...
 */
class PRelationImporter(dbPath: String, edgeFile: File, headFile: File) extends Importer with Logging{

  override val importerFileReader = new ImporterFileReader(edgeFile, 500000)
  override var propSortArr: Array[Int] = null
  override val headMap: Map[Int, String] = _setEdgeHead()

  override val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  service.scheduleAtFixedRate(importerFileReader.fillQueue, 0, 100, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 0, 1, TimeUnit.SECONDS)

  val relationDB = RocksDBStorage.getDB(s"${dbPath}/rels")
  val inRelationDB = RocksDBStorage.getDB(s"${dbPath}/inEdge")
  val outRelationDB = RocksDBStorage.getDB(s"${dbPath}/outEdge")
  val relationLabelDB = RocksDBStorage.getDB(s"${dbPath}/relLabelIndex")

  var globalCount: AtomicLong = new AtomicLong(0)
  val estEdgeCount: Long = estLineCount(edgeFile)

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)


  def importRelations(): Unit ={
    importData()
    logger.info(s"$globalCount relations imported.")
  }

  override protected def _importTask(taskId: Int): Boolean = {
    val serializer: RelationSerializer = new RelationSerializer()
    var innerCount = 0

    val inBatch = new WriteBatch()
    val outBatch = new WriteBatch()
    val storeBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines()
      batchData.foreach(line => {
        innerCount += 1
        val lineArr = line.replace("\n", "").split(",")
        val relation = _wrapEdge(lineArr)
        val serializedRel = serializer.serialize(relation)
        storeBatch.put(KeyHandler.relationKeyToBytes(relation.id), serializedRel)
        inBatch.put(KeyHandler.edgeKeyToBytes(relation.to, relation.typeId, relation.from), ByteUtils.longToBytes(relation.id))
        outBatch.put(KeyHandler.edgeKeyToBytes(relation.from, relation.typeId, relation.to), ByteUtils.longToBytes(relation.id))

        if(globalCount.get() % 1000000 == 0) {
          relationDB.write(writeOptions, storeBatch)
          inRelationDB.write(writeOptions, inBatch)
          outRelationDB.write(writeOptions, outBatch)
          storeBatch.clear()
          inBatch.clear()
          outBatch.clear()
        }
      })
      if(globalCount.addAndGet(batchData.length) % 10000000 == 0){
        val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
        logger.info(s"${globalCount.get() / 10000000}kw of $estEdgeCount(est) edges imported. $time1")
      }
      relationDB.write(writeOptions, storeBatch)
      inRelationDB.write(writeOptions, inBatch)
      outRelationDB.write(writeOptions, outBatch)
      storeBatch.clear()
      inBatch.clear()
      outBatch.clear()
    }
    val flushOptions = new FlushOptions
    relationDB.flush(flushOptions)
    inRelationDB.flush(flushOptions)
    outRelationDB.flush(flushOptions)
    logger.info(s"$innerCount, $taskId")
    true
  }

  private def _setEdgeHead(): Map[Int, String] = {
    _setHead(5, headFile)
  }

  private def _wrapEdge(lineArr: Array[String]): StoredRelationWithProperty = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))

    var propMap: Map[Int, Any] = Map[Int, Any]()
    for(i <-5 to lineArr.length -1) {
      val propId: Int = propSortArr(i - 5)
      val propValue: Any = {
        headMap(propId) match {
          case "long" => lineArr(i).toLong
          case "int" => lineArr(i).toInt
          case "boolean" => lineArr(i).toBoolean
          case "double" => lineArr(i).toDouble
          case _ => lineArr(i).replace("\"", "")
        }
      }
      propMap += (propId -> propValue)
    }
    new StoredRelationWithProperty(relId, fromId, toId, edgeType, propMap)
  }
}
