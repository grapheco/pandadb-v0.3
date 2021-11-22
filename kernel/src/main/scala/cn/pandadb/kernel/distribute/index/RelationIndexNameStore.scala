package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.{DistributedNameStore, NameMapping}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-22 14:29
 */
class RelationIndexNameStore(store: PandaDistributedIndexStore) extends DistributedNameStore {
  override val initInt: Int = 0
  override val indexStore: PandaDistributedIndexStore = store
  override val indexName: String = NameMapping.relationIndexMeta
  loadAll()
}
