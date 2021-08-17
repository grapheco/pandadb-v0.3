package cn.pandadb.kernel.transaction

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import cn.pandadb.kernel.kv.index.{TransactionIndexStore, TransactionIndexStoreAPI}
import cn.pandadb.kernel.kv.meta.TransactionStatistics
import cn.pandadb.kernel.kv.{TransactionGraphFacade, TransactionRocksDBStorage}
import cn.pandadb.kernel.kv.node.TransactionNodeStoreAPI
import cn.pandadb.kernel.kv.relation.TransactionRelationStoreAPI
import cn.pandadb.kernel.util.log.PandaUndoLogWriter
import com.typesafe.scalalogging.LazyLogging
import org.rocksdb.{ReadOptions, Snapshot, WriteOptions}


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

  val globalTransactionId = new AtomicLong(1)
  checkDir(nodeMetaDBPath)
  checkDir(nodeDBPath)
  checkDir(nodeLabelDBPath)
  checkDir(relationMetaDBPath)
  checkDir(relationDBPath)
  checkDir(inRelationDBPath)
  checkDir(outRelationDBPath)
  checkDir(relationLabelDBPath)
  checkDir(indexMetaDBPath)
  checkDir(indexDBPath)
  checkDir(fulltextIndexPath)
  checkDir(statisticsDBPath)

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

  private val logWriter = new PandaUndoLogWriter(undoLogFilePath + "/undo.txt")

  override def begin(): PandaTransaction = {
    val id = globalTransactionId.getAndIncrement()
    val writeOptions = new WriteOptions()
    val txMap = nodeStore.generateTransactions(writeOptions) ++ relationStore.generateTransactions(writeOptions) ++
      indexStore.generateTransactions(writeOptions) ++ statistics.generateTransactions(writeOptions)

    new PandaTransaction(s"$id", txMap, new TransactionGraphFacade(nodeStore, relationStore, indexStore, statistics, logWriter, {}))
  }

  def close(): Unit ={
    logWriter.close()
    statistics.close()
    indexStore.close()
    nodeStore.close()
    relationStore.close()
  }
  private def checkDir(dir: String): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      file.mkdirs()
      logger.info(s"New created data path (${dir})")
    }
    else {
      if (!file.isDirectory) {
        throw new Exception(s"The data path (${dir}) is invalid: not directory")
      }
    }
  }
}
