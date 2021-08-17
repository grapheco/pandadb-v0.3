package cn.pandadb.transaction

import java.io.File

import cn.pandadb.kernel.kv.TransactionRocksDBStorage
import cn.pandadb.kernel.transaction.PandaTransactionManager
import cn.pandadb.kernel.util.log.PandaLogReader
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.{ReadOptions, Snapshot}

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
    val undoLogFilePath = "./testinput/panda"

    transactionManager = new PandaTransactionManager(nodeMetaDBPath, nodeDBPath,nodeLabelDBPath,
      relationMetaDBPath,relationDBPath, inRelationDBPath,outRelationDBPath,relationLabelDBPath,
      indexMetaDBPath,indexDBPath,fulltextIndexPath,statisticsDBPath, undoLogFilePath)
  }

  @Test
  def testUndoLog(): Unit ={
    var tx = transactionManager.begin()
    tx.execute("create (n:person{name:'glx'}) return n", Map.empty).show() // memory data
    tx.execute("create (n:City{name:'China'}) return n", Map.empty).show() // memory data
    tx.execute(
      """
        |match (n:person)
        |match (m:City)
        |create (n)-[r:know]->(m)
        |""".stripMargin, Map.empty)
    tx.commit() // commit tx, data flush
    tx = transactionManager.begin()
    var res = tx.execute("match (n) return n", Map.empty).records() // search db data
    tx.commit()
    Assert.assertEquals(2, res.size)

    // recover
    tx = transactionManager.begin()
    val reader = new PandaLogReader("./testinput/panda/undo.txt")
    reader.recover(tx.rocksTxMap)
    res = tx.execute("match (n) return n", Map.empty).records()
    Assert.assertEquals(0, res.size)
    res = tx.execute("match (n)-[r]->(m) return r", Map.empty).records()
    Assert.assertEquals(0, res.size)
    transactionManager.close()
  }

  @Test
  def queryCreatedNodeInSameTx(): Unit ={
    val tx = transactionManager.begin()
    tx.execute("create (n:person{name:'glx'}) return n", Map.empty)
    val res = tx.execute("match (n) return n", Map.empty).records()
    tx.commit()
    transactionManager.close()
    Assert.assertEquals(1, res.size)
  }

}
