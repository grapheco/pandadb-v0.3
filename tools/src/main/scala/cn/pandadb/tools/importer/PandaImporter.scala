package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.meta.Statistics
import com.typesafe.scalalogging.LazyLogging

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:20 2020/12/9
 * @Modified By:
 */
object PandaImporter extends LazyLogging {
  val globalNodeCount = new AtomicLong(0)
  val globalNodePropCount = new AtomicLong(0)
  val globalRelCount = new AtomicLong(0)
  val globalRelPropCount = new AtomicLong(0)

  def time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)

  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  val nodeCountProgressLogger: Runnable = new Runnable {
    override def run(): Unit = {
      logger.info(s"$globalNodeCount nodes imported. $time")
    }
  }

  val relCountProgressLogger: Runnable = new Runnable {
    override def run(): Unit = {
      logger.info(s"$globalRelCount relations imported. $time")
    }
  }

  def main(args: Array[String]): Unit = {
    val startTime: Long = new Date().getTime
    val importCmd: ImportCmd = ImportCmd(args)
    val statistics: Statistics = new Statistics(importCmd.exportDBPath.getAbsolutePath)

    val estNodeCount: Long = {
      importCmd.nodeFileList.map(file => {
        CSVIOTools.estLineCount(file)
      }).sum
    }
    val estRelCount: Long = {
      importCmd.relFileList.map(file => {
        CSVIOTools.estLineCount(file)
      }).sum
    }

    logger.info(s"Estimated node count: $estNodeCount.")
    logger.info(s"Estimated relation count: $estRelCount.")

    val nodeDB = RocksDBStorage.getDB(path = importCmd.nodeDBPath, rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val nodeLabelDB = RocksDBStorage.getDB(importCmd.nodeLabelDBPath, rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val relationDB = RocksDBStorage.getDB(importCmd.relationDBPath, rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val inRelationDB = RocksDBStorage.getDB(importCmd.inRelationDBPath, rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val outRelationDB = RocksDBStorage.getDB(importCmd.outRelationDBPath, rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val relationTypeDB = RocksDBStorage.getDB(importCmd.relationTypeDBPath, rocksdbConfigPath = importCmd.rocksDBConfFilePath)

    val globalArgs = GlobalArgs(Runtime.getRuntime().availableProcessors(),
      globalNodeCount, globalNodePropCount,
      globalRelCount, globalRelPropCount,
      estNodeCount, estRelCount,
      nodeDB, nodeLabelDB = nodeLabelDB,
      relationDB = relationDB, inrelationDB = inRelationDB,
      outRelationDB = outRelationDB, relationTypeDB = relationTypeDB, statistics
    )
    logger.info(s"Import task started. $time")
    service.scheduleAtFixedRate(nodeCountProgressLogger, 0, 30, TimeUnit.SECONDS)
    service.scheduleAtFixedRate(relCountProgressLogger, 0, 30, TimeUnit.SECONDS)

    importCmd.nodeFileList.foreach(file => new SingleNodeFileImporter(file, importCmd, globalArgs).importData())
    nodeDB.close()
    nodeLabelDB.close()
    logger.info(s"$globalNodeCount nodes imported. $time")
    logger.info(s"$globalNodePropCount props of node imported. $time")

    importCmd.relFileList.foreach(file => new SingleRelationFileImporter(file, importCmd, globalArgs).importData())
    relationDB.close()
    inRelationDB.close()
    outRelationDB.close()
    relationTypeDB.close()
    logger.info(s"$globalRelCount relations imported. $time")
    logger.info(s"$globalRelPropCount props of relation imported. $time")

    PDBMetaData.persist(importCmd.exportDBPath.getAbsolutePath)
    service.shutdown()
    val endTime: Long = new Date().getTime
    val timeUsed: String = TimeUtil.millsSecond2Time(endTime - startTime)

    globalArgs.statistics.flush()

    logger.info(s"$globalNodeCount nodes imported. $time")
    logger.info(s"$globalNodePropCount props of node imported. $time")
    logger.info(s"$globalRelCount relations imported. $time")
    logger.info(s"$globalRelPropCount props of relation imported. $time")
    logger.info(s"Import task finished in $timeUsed")

  }
}