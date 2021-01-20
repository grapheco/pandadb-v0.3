package cn.pandadb.tools.importer

import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.apache.logging.log4j.scala.Logging

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
object PandaImporter extends Logging {
  val globalNodeCount = new AtomicLong(0)
  val globalNodePropCount = new AtomicLong(0)
  val globalRelCount = new AtomicLong(0)
  val globalRealPropCount = new AtomicLong(0)

  def time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)

  val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  val nodeCountProgressLogger: Runnable = new Runnable {
    override def run(): Unit = {
//      logger.info(s"$globalNodeCount nodes imported. $time")
      println(s"$globalNodeCount nodes imported. $time")
    }
  }

  val relCountProgressLogger: Runnable = new Runnable {
    override def run(): Unit = {
//      logger.info(s"$globalRelCount relations imported. $time")
      println(s"$globalRelCount relations imported. $time")
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

    println(s"Estimated node count: $estNodeCount.")
    println(s"Estimated relation count: $estRelCount.")

    val nodeDB = RocksDBStorage.getDB(path = s"${importCmd.database}/nodes", useForImporter = true, isHDD = true)
    val nodeLabelDB = RocksDBStorage.getDB(s"${importCmd.database}/nodeLabel", useForImporter = true, isHDD = true)
    val relationDB = RocksDBStorage.getDB(s"${importCmd.database}/rels", useForImporter = true, isHDD = true)
    val inRelationDB = RocksDBStorage.getDB(s"${importCmd.database}/inEdge", useForImporter = true, isHDD = true)
    val outRelationDB = RocksDBStorage.getDB(s"${importCmd.database}/outEdge", useForImporter = true, isHDD = true)
    val relationTypeDB = RocksDBStorage.getDB(s"${importCmd.database}/relLabelIndex", useForImporter = true, isHDD = true)

    val globalArgs = GlobalArgs(Runtime.getRuntime().availableProcessors(),
      globalNodeCount, globalNodePropCount,
      globalRelCount, globalRealPropCount,
      estNodeCount, estRelCount,
      nodeDB, nodeLabelDB = nodeLabelDB,
      relationDB = relationDB, inrelationDB = inRelationDB,
      outRelationDB = outRelationDB, relationTypeDB = relationTypeDB
    )

    println(s"Import task started. $time")
    service.scheduleAtFixedRate(nodeCountProgressLogger, 0, 30, TimeUnit.SECONDS)
    service.scheduleAtFixedRate(relCountProgressLogger, 0, 30, TimeUnit.SECONDS)

    importCmd.nodeFileList.map(file => new SingleNodeFileImporter(file, importCmd, globalArgs).importData())
    nodeDB.close()
    nodeLabelDB.close()
    println(s"$globalNodeCount nodes imported. $time")
    println(s"$globalNodePropCount props of node imported. $time")

    importCmd.relFileList.map(file => new SingleRelationFileImporter(file, importCmd, globalArgs).importData())
    relationDB.close()
    inRelationDB.close()
    outRelationDB.close()
    relationTypeDB.close()
    println(s"$globalRelCount relations imported. $time")
    println(s"$globalRealPropCount props of relation imported. $time")

    PDBMetaData.persist(args(0))
    service.shutdown()
    val endTime: Long = new Date().getTime
    val timeUsed: String = TimeUtil.millsSecond2Time(endTime - startTime)
//    logger.info("import task finished.")
    println(s"$globalNodeCount nodes imported. $time")
    println(s"$globalNodePropCount props of node imported. $time")
    println(s"$globalRelCount relations imported. $time")
    println(s"$globalRealPropCount props of relation imported. $time")
    println(s"Import task finished in $timeUsed")

  }
}