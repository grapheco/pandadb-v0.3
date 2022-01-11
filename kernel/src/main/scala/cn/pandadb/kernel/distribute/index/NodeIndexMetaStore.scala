package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.{NodeLabelNameStore, PropertyNameStore}
import cn.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import cn.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-22 14:28
 */
class NodeIndexMetaStore(_db: DistributedKVAPI, nls: DistributedNodeStoreSPI) extends IndexNameStore {
  override val db: DistributedKVAPI = _db
  override val keyPrefixFunc: () => Array[Byte] = DistributedKeyConverter.indexMetaPrefixToBytes
  override val encodingKeyPrefix: () => Array[Byte] = ()=>Array(DistributedKeyConverter.indexEncoderPrefix)
  override val keyWithLabelPrefixFunc: Int => Array[Byte] = DistributedKeyConverter.indexMetaWithLabelPrefixToBytes
  override val keyWithIndexFunc: (Int, Int) => Array[Byte] = DistributedKeyConverter.indexMetaToBytes
  override val nodeStore: DistributedNodeStoreSPI = nls
  loadAll()
}
