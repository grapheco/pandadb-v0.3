package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.{DistributedNameStore, NameMapping}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-22 14:29
 */
class RelationIndexMetaStore(store: PandaDistributedIndexStore) extends DistributedIndexMetaStore {
  override val indexStore: DistributedIndexStore = store
  override val indexName: String = NameMapping.relationIndexMeta
  loadAll()
}
