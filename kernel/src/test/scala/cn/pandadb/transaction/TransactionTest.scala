package cn.pandadb.transaction

import java.io.{BufferedWriter, File, FileWriter, RandomAccessFile}

import cn.pandadb.kernel.kv.TransactionRocksDBStorage
import cn.pandadb.kernel.transaction.PandaTransactionManager
import cn.pandadb.kernel.util.log.PandaLog
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.{ReadOptions, Snapshot, WriteOptions}

import scala.io.Source

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-12 13:53
 */
class TransactionTest {
  var transactionManager: PandaTransactionManager = _

  @Before
  def init(): Unit ={
    FileUtils.deleteDirectory(new File("./testinput/panda"))

    transactionManager = new PandaTransactionManager("./testinput/panda")
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

    transactionManager.close()
  }

  @Test
  def queryCreatedNodeInSameTx(): Unit ={
    val tx = transactionManager.begin()
    tx.execute("create (n:person{name:'glx'}) return n", Map.empty)
    val res = tx.execute("match (n) return n", Map.empty).records()
    Assert.assertEquals(1, res.size)
    tx.commit()
    transactionManager.close()
  }

  @Test
  def concurrentTest(): Unit ={
    (1 to 5).par.foreach{f =>
      val tx = transactionManager.begin()
      tx.execute(s"create (n:person{age:$f}) return n", Map.empty)
      tx.commit()
    }

    val tx = transactionManager.begin()
    tx.execute("match (n) return n", Map.empty).show()
    tx.commit()

//    (1 to 5).par.foreach{f =>
//      val tx = transactionManager.begin()
//      tx.execute(s"match (n{age:5}) set n.name=$f return n", Map.empty).show()
//      tx.commit()
//    }

    transactionManager.close()
  }

  @Test
  def t(): Unit ={
    val db = TransactionRocksDBStorage.getDB("./testinput/panda/tset.db")
    db.put(Array(1.toByte), Array(11.toByte))
    val writeOptions = new WriteOptions()

    val tx1 = db.beginTransaction(writeOptions)
    val tx2 = db.beginTransaction(writeOptions)
    tx1.put(Array(1.toByte), Array(22.toByte))
    tx2.put(Array(1.toByte), Array(33.toByte))
    tx1.commit()
    tx2.commit()
    println(db.get(Array(1.toByte))(0).toInt)
  }

  @Test
  def testFileLock(): Unit ={
    val writer = new BufferedWriter(new FileWriter("./testinput/panda/tstLock.txt"))
    val raf = new RandomAccessFile("./testinput/panda/tstLock.txt", "rw")
    val fileChannel = raf.getChannel
    val lock = fileChannel.tryLock()
    if (lock.isValid){
      raf.writeChars("1")
    }
    lock.release()
  }
}
