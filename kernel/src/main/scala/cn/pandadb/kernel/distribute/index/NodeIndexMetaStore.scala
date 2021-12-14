package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.{NameMapping}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-22 14:28
 */
class NodeIndexMetaStore(store: PandaDistributedIndexStore) extends DistributedIndexMetaStore {
  override val indexStore: DistributedIndexStore = store
  override val indexName: String = NameMapping.nodeIndexMeta
  loadAll()
}
