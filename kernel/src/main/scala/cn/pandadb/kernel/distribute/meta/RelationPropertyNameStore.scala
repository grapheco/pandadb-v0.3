package cn.pandadb.kernel.distribute.meta

import cn.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:52
 */
class RelationPropertyNameStore(_db: DistributedKVAPI) extends DistributedNameStore {
  override val initInt: Int = 0
  override val db: DistributedKVAPI = _db
  override val key2ByteArrayFunc: Int => Array[Byte] = DistributedKeyConverter.propertyNameKeyToBytes
  override val keyPrefixFunc: () => Array[Byte] = DistributedKeyConverter.propertyNameKeyPrefixToBytes
  loadAll()
}
