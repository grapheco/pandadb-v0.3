package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.meta.Statistics
import cn.pandadb.kernel.util.DBNameMap
import com.typesafe.scalalogging.LazyLogging

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:20 2020/12/9
 * @Modified By:
 */
object PandaImporter extends LazyLogging {

  val importerStatics: ImporterStatics = new ImporterStatics

  def time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)

  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  val nodeCountProgressLogger: Runnable = new Runnable {
    override def run(): Unit = {
      logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    }
  }

  val relCountProgressLogger: Runnable = new Runnable {
    override def run(): Unit = {
      logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
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

    val nodeDB = RocksDBStorage.getDB(path = {if(importCmd.advancdeMode) importCmd.nodeDBPath else s"${importCmd.database}/${DBNameMap.nodeDB}"}, rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val nodeLabelDB = RocksDBStorage.getDB(if(importCmd.advancdeMode) importCmd.nodeLabelDBPath else s"${importCmd.database}/${DBNameMap.nodeLabelDB}", rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val relationDB = RocksDBStorage.getDB(if(importCmd.advancdeMode) importCmd.relationDBPath else s"${importCmd.database}/${DBNameMap.relationDB}", rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val inRelationDB = RocksDBStorage.getDB(if(importCmd.advancdeMode) importCmd.inRelationDBPath else s"${importCmd.database}/${DBNameMap.inRelationDB}", rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val outRelationDB = RocksDBStorage.getDB(if(importCmd.advancdeMode) importCmd.outRelationDBPath else s"${importCmd.database}/${DBNameMap.outRelationDB}", rocksdbConfigPath = importCmd.rocksDBConfFilePath)
    val relationTypeDB = RocksDBStorage.getDB(if(importCmd.advancdeMode) importCmd.relationTypeDBPath else s"${importCmd.database}/${DBNameMap.relationLabelDB}", rocksdbConfigPath = importCmd.rocksDBConfFilePath)


    val globalArgs = GlobalArgs(Runtime.getRuntime().availableProcessors(),
      importerStatics,
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
    logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    logger.info(s"${importerStatics.getGlobalNodePropCount} props of node imported. $time")

    importCmd.relFileList.foreach(file => new SingleRelationFileImporter(file, importCmd, globalArgs).importData())
    relationDB.close()
    inRelationDB.close()
    outRelationDB.close()
    relationTypeDB.close()
    logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
    logger.info(s"${importerStatics.getGlobalRelPropCount} props of relation imported. $time")

    PDBMetaData.persist(importCmd.exportDBPath.getAbsolutePath)
    service.shutdown()
    val endTime: Long = new Date().getTime
    val timeUsed: String = TimeUtil.millsSecond2Time(endTime - startTime)

    importerStatics.getNodeCountByLabel.foreach(kv => globalArgs.statistics.setNodeLabelCount(kv._1, kv._2))
    importerStatics.getRelCountByType.foreach(kv => globalArgs.statistics.setRelationTypeCount(kv._1, kv._2))
    globalArgs.statistics.nodeCount  = importerStatics.getGlobalNodeCount.get()
    globalArgs.statistics.relationCount = importerStatics.getGlobalRelCount.get()
    globalArgs.statistics.flush()

    logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    logger.info(s"${importerStatics.getGlobalNodePropCount} props of node imported. $time")
    logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
    logger.info(s"${importerStatics.getGlobalRelPropCount} props of relation imported. $time")
    logger.info(s"Import task finished in $timeUsed")

  }
}