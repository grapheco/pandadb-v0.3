package cn.pandadb.kernel.util.log

import java.io.FileWriter

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-16 14:01
 */
trait LogWriter {
  val filePath: String
  def writeUndoLog(txId: String, dbName: String, key: Array[Byte], oldValue: Array[Byte]): Unit
  def flush()
  def close()
}

class PandaUndoLogWriter(path: String) extends LogWriter{
  override val filePath: String = path
  private val writer = new FileWriter(filePath, true)

  override def writeUndoLog(txId: String, dbName: String, key: Array[Byte], oldValue: Array[Byte]): Unit ={
    // todo: oldValue = null
    writer.write(s"$txId~$dbName~${transByteArray(key)}~${transByteArray(oldValue)}\n")
  }

  def flush(): Unit ={
    writer.flush()
  }
  def close(): Unit ={
    writer.close()
  }

  private def transByteArray(data: Array[Byte]): String ={
    if (data == null || (data sameElements Array.emptyByteArray)) return null
    val res = data.foldLeft("[")((res, byte) =>{
      res + byte.toString + ","
    })
    res.slice(0, res.length - 1) + "]"
  }
}