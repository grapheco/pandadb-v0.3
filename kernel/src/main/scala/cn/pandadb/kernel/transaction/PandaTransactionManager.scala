package cn.pandadb.kernel.transaction

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.kernel.kv.index.{TransactionIndexStore, TransactionIndexStoreAPI}
import cn.pandadb.kernel.kv.meta.TransactionStatistics
import cn.pandadb.kernel.kv.{TransactionGraphFacade, TransactionRocksDBStorage}
import cn.pandadb.kernel.kv.node.TransactionNodeStoreAPI
import cn.pandadb.kernel.kv.relation.TransactionRelationStoreAPI
import cn.pandadb.kernel.util.CommonUtils
import cn.pandadb.kernel.util.log.{PandaLog}
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
                              undoLogFilePath: String,
                              rocksDBConfigPath: String = "default") extends TransactionManager with LazyLogging {

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
  CommonUtils.checkDir(undoLogFilePath)


  private val nodeDB = TransactionRocksDBStorage.getDB(nodeDBPath)
  private val nodeLabelDB = TransactionRocksDBStorage.getDB(nodeLabelDBPath)
  private val nodeMetaDB = TransactionRocksDBStorage.getDB(nodeMetaDBPath)
  private val nodeStore = new TransactionNodeStoreAPI(nodeDB, nodeLabelDB, nodeMetaDB)

  private val relationDB = TransactionRocksDBStorage.getDB(relationDBPath)
  private val inRelationDB = TransactionRocksDBStorage.getDB(inRelationDBPath)
  private val outRelationDB = TransactionRocksDBStorage.getDB(outRelationDBPath)
  private val relationLabelDB = TransactionRocksDBStorage.getDB(relationLabelDBPath)
  private val relationMetaDB = TransactionRocksDBStorage.getDB(relationMetaDBPath)
  private val relationStore = new TransactionRelationStoreAPI(relationDB, inRelationDB, outRelationDB, relationLabelDB, relationMetaDB)

  private val indexDB = TransactionRocksDBStorage.getDB(indexDBPath)
  private val indexMetaDB = TransactionRocksDBStorage.getDB(indexMetaDBPath)
  private val indexStore = new TransactionIndexStoreAPI(indexMetaDB, indexDB, fulltextIndexPath)

  private val statistics = new TransactionStatistics(TransactionRocksDBStorage.getDB(statisticsDBPath))

  private val pandaLog = new PandaLog(undoLogFilePath)

  private val globalTransactionId = new AtomicLong(pandaLog.recoverDB(getTransactions()))

  override def begin(): PandaTransaction = {
    val id = globalTransactionId.getAndIncrement()
    val txMap = getTransactions()
    new PandaTransaction(s"$id", txMap, new TransactionGraphFacade(nodeStore, relationStore, indexStore, statistics, pandaLog, {}))
  }

  def getTransactions(): Map[String, Transaction] ={
    val writeOptions = new WriteOptions()
    nodeStore.generateTransactions(writeOptions) ++ relationStore.generateTransactions(writeOptions) ++
      indexStore.generateTransactions(writeOptions) ++ statistics.generateTransactions(writeOptions)
  }

  def close(): Unit ={
    pandaLog.close()
    statistics.close()
    indexStore.close()
    nodeStore.close()
    relationStore.close()
  }
}
