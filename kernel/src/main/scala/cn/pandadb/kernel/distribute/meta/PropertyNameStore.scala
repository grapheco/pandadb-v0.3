package cn.pandadb.kernel.distribute.meta

import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 16:52
 */
class PropertyNameStore(store: PandaDistributedIndexStore) extends DistributedNameStore {
  override val initInt: Int = 0
  override val indexStore: PandaDistributedIndexStore = store
  override val indexName: String = MetaNameMapping.propertyMetaName
  loadAll()
}
