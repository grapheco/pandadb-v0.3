package cn.pandadb.tools.importer

import com.typesafe.scalalogging.LazyLogging
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import cn.pandadb.kernel.distribute.PandaDistributeKVAPI
import cn.pandadb.kernel.distribute.meta.DistributedStatistics
import org.tikv.common.{TiConfiguration, TiSession}

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

    val db = {
      val conf = TiConfiguration.createRawDefault(importCmd.kvHosts)
      val session = TiSession.create(conf)
      new PandaDistributeKVAPI(session.createRawClient())
    }
    val statistics = new DistributedStatistics(db)
    statistics.init()

    val globalArgs = GlobalArgs(Runtime.getRuntime().availableProcessors(),
      importerStatics,
      estNodeCount, estRelCount, db)
    logger.info(s"Import task started. $time")
    service.scheduleAtFixedRate(nodeCountProgressLogger, 0, 30, TimeUnit.SECONDS)
    service.scheduleAtFixedRate(relCountProgressLogger, 0, 30, TimeUnit.SECONDS)

    importCmd.nodeFileList.foreach(file => new SingleNodeFileImporter(file, importCmd, globalArgs).importData())

    logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    logger.info(s"${importerStatics.getGlobalNodePropCount} props of node imported. $time")

    importCmd.relFileList.foreach(file => new SingleRelationFileImporter(file, importCmd, globalArgs).importData())

    logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
    logger.info(s"${importerStatics.getGlobalRelPropCount} props of relation imported. $time")

//    PDBMetaData.persist(globalArgs)
    statistics.increaseNodeCount(importerStatics.getGlobalNodeCount.get())
    importerStatics.getNodeCountByLabel.foreach(idNums=> statistics.increaseNodeLabelCount(idNums._1, idNums._2))
    statistics.increaseRelationCount(importerStatics.getGlobalRelCount.get())
    importerStatics.getRelCountByType.foreach(idNums => statistics.increaseRelationTypeCount(idNums._1, idNums._2))
    statistics.flush()

    service.shutdown()
    val endTime: Long = new Date().getTime
    val timeUsed: String = TimeUtil.millsSecond2Time(endTime - startTime)


    logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    logger.info(s"${importerStatics.getGlobalNodePropCount} props of node imported. $time")
    logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
    logger.info(s"${importerStatics.getGlobalRelPropCount} props of relation imported. $time")
    logger.info(s"Import task finished in $timeUsed")

  }
}