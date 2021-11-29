package cn.pandadb.tools.importer

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import cn.pandadb.kernel.{DistributedPDBMetaData, PDBMetaData}
import cn.pandadb.kernel.distribute.PandaDistributeKVAPI
import com.typesafe.scalalogging.LazyLogging
import org.tikv.common.{TiConfiguration, TiSession}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 20:20 2020/12/9
 * @Modified By:
 */
object DistributedPandaImporter extends LazyLogging {

  val importerStatics: DistributedImporterStatics = new DistributedImporterStatics

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
      val conf = TiConfiguration.createRawDefault("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379")
      val session = TiSession.create(conf)
      session.createRawClient()
      new PandaDistributeKVAPI(session.createRawClient())
    }


    val globalArgs = DistributedGlobalArgs(Runtime.getRuntime().availableProcessors(),
      importerStatics,
      estNodeCount, estRelCount,
      db, relationDB = db, inrelationDB = db, outRelationDB = db)
    logger.info(s"Import task started. $time")
    service.scheduleAtFixedRate(nodeCountProgressLogger, 0, 30, TimeUnit.SECONDS)
    service.scheduleAtFixedRate(relCountProgressLogger, 0, 30, TimeUnit.SECONDS)

    importCmd.nodeFileList.foreach(file => new DistributedSingleNodeFileImporter(file, importCmd, globalArgs).importData())

    logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    logger.info(s"${importerStatics.getGlobalNodePropCount} props of node imported. $time")

    importCmd.relFileList.foreach(file => new DistributedSingleRelationFileImporter(file, importCmd, globalArgs).importData())

    logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
    logger.info(s"${importerStatics.getGlobalRelPropCount} props of relation imported. $time")

    DistributedPDBMetaData.persist()
    service.shutdown()
    val endTime: Long = new Date().getTime
    val timeUsed: String = TimeUtil.millsSecond2Time(endTime - startTime)

//    globalArgs.statistics.flush()

    logger.info(s"${importerStatics.getGlobalNodeCount} nodes imported. $time")
    logger.info(s"${importerStatics.getGlobalNodePropCount} props of node imported. $time")
    logger.info(s"${importerStatics.getGlobalRelCount} relations imported. $time")
    logger.info(s"${importerStatics.getGlobalRelPropCount} props of relation imported. $time")
    logger.info(s"Import task finished in $timeUsed")
  }
}