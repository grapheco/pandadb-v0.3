package cn.pandadb.kernel.transaction

import java.io.File
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import cn.pandadb.kernel.kv.index.{TransactionIndexStore, TransactionIndexStoreAPI}
import cn.pandadb.kernel.kv.meta.TransactionStatistics
import cn.pandadb.kernel.kv.{TransactionGraphFacade, TransactionRocksDBStorage}
import cn.pandadb.kernel.kv.node.TransactionNodeStoreAPI
import cn.pandadb.kernel.kv.relation.TransactionRelationStoreAPI
import cn.pandadb.kernel.util.{CommonUtils, DBNameMap}
import cn.pandadb.kernel.util.log.PandaLog
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.rocksdb.{ReadOptions, Snapshot, Transaction, WriteOptions}

import scala.io.Source


/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-11 16:24
 */
class PandaTransactionManager(nodeMetaDBPath: String,
                              nodeDBPath: String,
                              nodeLabelDBPath: String,
                              relationMetaDBPath: String,
                              relationDBPath: String,
                              inRelationDBPath: String,
                              outRelationDBPath: String,
                              relationLabelDBPath: String,
                              indexMetaDBPath: String,
                              indexDBPath: String,
                              fulltextIndexPath: String,
                              statisticsDBPath: String,
                              pandaLogFilePath: String,
                              rocksDBConfigPath: String) extends TransactionManager with LazyLogging {

  def this(dbPath: String, rocksdbConfigPath: String = "default") {
    this(dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, dbPath, rocksdbConfigPath)
  }

  CommonUtils.checkDir(nodeMetaDBPath)
  CommonUtils.checkDir(nodeDBPath)
  CommonUtils.checkDir(nodeLabelDBPath)
  CommonUtils.checkDir(relationMetaDBPath)
  CommonUtils.checkDir(relationDBPath)
  CommonUtils.checkDir(inRelationDBPath)
  CommonUtils.checkDir(outRelationDBPath)
  CommonUtils.checkDir(relationLabelDBPath)
  CommonUtils.checkDir(indexMetaDBPath)
  CommonUtils.checkDir(indexDBPath)
  CommonUtils.checkDir(fulltextIndexPath)
  CommonUtils.checkDir(statisticsDBPath)
  CommonUtils.checkDir(pandaLogFilePath)

  private val txWatcher = new TransactionWatcher(pandaLogFilePath)
  private val pandaLog = new PandaLog(pandaLogFilePath, txWatcher)

  private val nodeDB = TransactionRocksDBStorage.getDB(nodeDBPath + s"/${DBNameMap.nodeDB}")
  private val nodeLabelDB = TransactionRocksDBStorage.getDB(nodeLabelDBPath + s"/${DBNameMap.nodeLabelDB}")
  private val nodeMetaDB = TransactionRocksDBStorage.getDB(nodeMetaDBPath + s"/${DBNameMap.nodeMetaDB}")
  private val nodeStore = new TransactionNodeStoreAPI(nodeDB, nodeLabelDB, nodeMetaDB, pandaLog)

  private val relationDB = TransactionRocksDBStorage.getDB(relationDBPath + s"/${DBNameMap.relationDB}")
  private val inRelationDB = TransactionRocksDBStorage.getDB(inRelationDBPath + s"/${DBNameMap.inRelationDB}")
  private val outRelationDB = TransactionRocksDBStorage.getDB(outRelationDBPath + s"/${DBNameMap.outRelationDB}")
  private val relationLabelDB = TransactionRocksDBStorage.getDB(relationLabelDBPath + s"/${DBNameMap.relationLabelDB}")
  private val relationMetaDB = TransactionRocksDBStorage.getDB(relationMetaDBPath + s"/${DBNameMap.relationMetaDB}")
  private val relationStore = new TransactionRelationStoreAPI(relationDB, inRelationDB, outRelationDB, relationLabelDB, relationMetaDB, pandaLog)

  private val indexDB = TransactionRocksDBStorage.getDB(indexDBPath + s"/${DBNameMap.indexDB}")
  private val indexMetaDB = TransactionRocksDBStorage.getDB(indexMetaDBPath + s"/${DBNameMap.indexMetaDB}")
  private val indexStore = new TransactionIndexStoreAPI(indexMetaDB, indexDB, fulltextIndexPath, pandaLog)

  private val statistics = new TransactionStatistics(TransactionRocksDBStorage.getDB(statisticsDBPath + s"/${DBNameMap.statisticsDB}"), pandaLog)

  private val globalTransactionId = new AtomicLong(pandaLog.recoverDB(getTransactions()))

  statistics.init()

  val graphFacade = new TransactionGraphFacade(nodeStore, relationStore, indexStore, statistics, pandaLog, {})

  val schedule = Executors.newSingleThreadScheduledExecutor()
  schedule.scheduleAtFixedRate(txWatcher, 60, 60, TimeUnit.SECONDS)

  override def begin(): PandaTransaction = {
    val id = globalTransactionId.getAndIncrement()
    val txMap = getTransactions()
    new PandaTransaction(s"$id", txMap, graphFacade, txWatcher)
  }

  private def getTransactions(): Map[String, Transaction] = {
    val writeOptions = new WriteOptions()
    nodeStore.generateTransactions(writeOptions) ++ relationStore.generateTransactions(writeOptions) ++
      indexStore.generateTransactions(writeOptions) ++ statistics.generateTransactions(writeOptions)
  }

  def close(): Unit = {
    pandaLog.close()
    statistics.close()
    indexStore.close()
    nodeStore.close()
    relationStore.close()
  }
}
