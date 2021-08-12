package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.TransactionDB

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-09 17:30
 */
class TransactionNodeLabelNameStore(tdb: TransactionDB) extends TransactionNameStore {
  override val db: TransactionDB = tdb
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.nodeLabelKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.nodeLabelKeyPrefixToBytes
  override val initInt: Int = 100000
  loadAll()
}
