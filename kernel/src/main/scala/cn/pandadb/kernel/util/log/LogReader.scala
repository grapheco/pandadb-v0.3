package cn.pandadb.kernel.util.log

import java.io.File

import org.rocksdb.Transaction

import scala.io.Source

trait LogReader {

}

class PandaLogReader(path: String) extends LogReader{
  val log = Source.fromFile(new File(path))

  def recover(txMap: Map[String, Transaction]): Unit ={
    parseLogLine().foreach(line => {
      if (line._4 == null){
        txMap(line._2).delete(line._3)
      }
      else txMap(line._2).put(line._3, line._4)
    })

    txMap.values.foreach(_.commit())
  }

  def parseLogLine(): Iterator[(String, String, Array[Byte], Array[Byte])] ={
    val lines = log.getLines()

    new Iterator[(String, String, Array[Byte], Array[Byte])]{
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
}
