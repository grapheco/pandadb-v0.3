package cn.pandadb.transaction

import java.io.File

import cn.pandadb.kernel.transaction.PandaTransactionManager
import org.apache.commons.io.FileUtils
import org.junit.{After, Before, Test}

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-12 13:53
 */
class TransactionTest {
  var transactionManager: PandaTransactionManager = null

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File("./testinput/panda"))

    val nodeMetaDBPath = "./testinput/panda/nodeMeta.db"
    val nodeDBPath = "./testinput/panda/node.db"
    val nodeLabelDBPath = "./testinput/panda/nodeLabel.db"
    val relationMetaDBPath = "./testinput/panda/relationMeta.db"
    val relationDBPath = "./testinput/panda/relation.db"
    val inRelationDBPath = "./testinput/panda/inRelation.db"
    val outRelationDBPath = "./testinput/panda/outRelation.db"
    val relationLabelDBPath = "./testinput/panda/relationLabel.db"
    val indexMetaDBPath = "./testinput/panda/indexMeta.db"
    val indexDBPath = "./testinput/panda/index.db"
    val fulltextIndexPath = "./testinput/panda/fulltextIndex.db"
    val statisticsDBPath = "./testinput/panda/statistics.db"

    transactionManager = new PandaTransactionManager(nodeMetaDBPath, nodeDBPath,nodeLabelDBPath,
      relationMetaDBPath,relationDBPath, inRelationDBPath,outRelationDBPath,relationLabelDBPath,
      indexMetaDBPath,indexDBPath,fulltextIndexPath,statisticsDBPath)
  }

  @Test
  def test(): Unit ={
    var tx = transactionManager.begin()
    tx.execute("create (n:person{name:'glx'}) return n")
    tx.rollback()
    println("=========================")
    tx = transactionManager.begin()
    tx.execute("match (n) return n")
    tx.commit()
    transactionManager.close()
  }
}
