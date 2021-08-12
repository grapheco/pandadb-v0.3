package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.db.KeyValueDB
import org.rocksdb.TransactionDB

/**
 * @program: pandadb-v0.3
 * @description: PandaDB No.1
 * @author: LiamGao
 * @create: 2021-08-09 17:31
 */
class TransactionPropertyNameStore(tdb: TransactionDB) extends TransactionNameStore {
  override val db: TransactionDB = tdb
  override val key2ByteArrayFunc: Int => Array[Byte] = KeyConverter.propertyNameKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = KeyConverter.propertyNameKeyPrefixToBytes
  override val initInt: Int = 200000
  loadAll()
}
