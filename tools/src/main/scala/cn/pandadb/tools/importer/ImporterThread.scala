package cn.pandadb.tools.importer

import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.rocksdb.{WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 15:29 2020/12/22
 * @Modified By:
 */
trait ImporterThread extends Thread {
  val iter: Iterator[String]
  val writeOpt = new WriteOptions()
  val batch = new WriteBatch()
  val serializer: BaseSerializer
  val batchSize: Int
}
