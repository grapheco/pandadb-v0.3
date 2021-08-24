package cn.pandadb.kernel

import cn.pandadb.kernel.transaction.PandaTransactionManager

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-23 15:04
 */
object TransactionGraphDatabaseBuilder {

  def newEmbeddedDatabase(dbPath: String, rocksdbConfPath: String = "default"): PandaTransactionManager ={
    new PandaTransactionManager(dbPath, rocksdbConfPath)
  }

  def newEmbeddedDatabase(nodeMetaDBPath: String,
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
                          rocksDBConfigPath:String,
                          pandaLogPath: String): PandaTransactionManager ={

    new PandaTransactionManager(
      nodeMetaDBPath, nodeDBPath, nodeLabelDBPath,
      relationMetaDBPath, relationDBPath, inRelationDBPath, outRelationDBPath, relationLabelDBPath,
      indexMetaDBPath, indexDBPath, fulltextIndexPath,
      statisticsDBPath, pandaLogPath, rocksDBConfigPath)
  }
}
