package cn.pandadb.kernel.util.log

import java.io.{BufferedWriter, File, FileWriter, RandomAccessFile}

import cn.pandadb.kernel.transaction.TransactionWatcher
import cn.pandadb.kernel.util.{CommonUtils, DBNameMap}
import com.typesafe.scalalogging.LazyLogging
import org.rocksdb.Transaction

import scala.io.Source
import scala.util.control.Breaks

/**
 * @program: pandadb-v0.3
 * @description: undo log: log the value before update
 *               guard log: ensure transaction success, other wise, recover db.
 * @author: LiamGao
 * @create: 2021-08-19 09:12
 */

class PandaLog(logPath: String, txWatcher: TransactionWatcher) extends LazyLogging {
  private var writeTxId: String = ""

  private val undoLogPath = s"$logPath/${DBNameMap.undoLogName}"
  private val guardLogPath = s"$logPath/${DBNameMap.guardLogName}"

  CommonUtils.checkDir(logPath)
  CommonUtils.createFile(undoLogPath)
  CommonUtils.createFile(guardLogPath)

  private val undoLogWriter = new BufferedWriter(new FileWriter(undoLogPath, true))
  private val guardLogWriter = new BufferedWriter(new FileWriter(guardLogPath, true))


  def writeUndoLog(txId: String, dbName: String, key: Array[Byte], oldValue: Array[Byte]): Unit = {
    this.synchronized {
      while (txWatcher.isCleaning){
        Thread.sleep(100)
      }
      val line = s"$txId~$dbName~${transByteArray(key)}~${transByteArray(oldValue)}"
      undoLogWriter.write(line)
      undoLogWriter.newLine()
      writeTxId = txId
    }
  }

  def writeGuardLog(txId: String): Unit = {
    this.synchronized {
      while (txWatcher.isCleaning){
        Thread.sleep(100)
      }
      guardLogWriter.write(txId)
      guardLogWriter.newLine()
    }
  }

  def recoverDB(txMap: Map[String, Transaction]): Long = {
    logger.info("............Checking db status............")
    val undoData = parseUndoLogLine().toArray.groupBy(f => f._1)
    val guardData = parseGuardLogLine()
    val waitToRecoverTxId = undoData.filter(line => !guardData.contains(line._1)).toArray.sortBy(f => f._1.toLong).reverse

    if (waitToRecoverTxId.nonEmpty) {
      waitToRecoverTxId.foreach(kv => {
        kv._2.foreach(line => {
          if (line._4 == null) {
            txMap(line._2).delete(line._3)
          }
          else txMap(line._2).put(line._3, line._4)
        })
      })
      txMap.values.foreach(_.commit())
      logger.info("............Recover db success............")
      CommonUtils.cleanFileContent(undoLogPath)
      CommonUtils.cleanFileContent(guardLogPath)
      1L
//      guardData.map(_.trim.toLong).max + 1
    }
    else {
      // normal start, clean undoLog and guardLog
      CommonUtils.cleanFileContent(undoLogPath)
      CommonUtils.cleanFileContent(guardLogPath)
      logger.info("............Checked db status............")
      1L
    }
  }

  private def parseUndoLogLine(): Iterator[(String, String, Array[Byte], Array[Byte])] = {
    val log = Source.fromFile(undoLogPath)
    val lines = log.getLines()

    new Iterator[(String, String, Array[Byte], Array[Byte])] {
      override def hasNext: Boolean = lines.hasNext

      override def next(): (String, String, Array[Byte], Array[Byte]) = {
        val data = lines.next().split("~")
        val txId = data(0)
        val dbName = data(1)
        val keyBytes = data(2).slice(1, data(2).length - 1).split(",").map(_.toByte)
        val value = {
          val v = data(3)
          if (v == "null") null
          else v.slice(1, v.length - 1).split(",").map(_.toByte)
        }
        (txId, dbName, keyBytes, value)
      }
    }
  }

  private def parseGuardLogLine(): Array[String] = {
    val guardFile = Source.fromFile(guardLogPath)
    guardFile.getLines().toArray
  }

  def flushUndoLog(): String = {
    undoLogWriter.flush()
    writeTxId
  }

  def flushGuardLog(): Unit = {
    guardLogWriter.flush()
  }

  def close(): Unit = {
    guardLogWriter.flush()
    undoLogWriter.flush()

    guardLogWriter.close()
    undoLogWriter.close()
  }

  private def transByteArray(data: Array[Byte]): String = {
    if (data == null || (data sameElements Array.emptyByteArray)) return null
    val res = data.foldLeft("[")((res, byte) => {
      res + byte.toString + ","
    })
    res.slice(0, res.length - 1) + "]"
  }
}
