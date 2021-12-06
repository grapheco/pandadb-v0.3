package cn.pandadb.kernel.distribute.statistics

import cn.pandadb.kernel.distribute.index.PandaDistributedIndexStore
import cn.pandadb.kernel.distribute.meta.NameMapping

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-24 15:09
 */
class IndexStatistics(indexStore: PandaDistributedIndexStore) {
  type LabelName = String
  type PropertyName = String

  val indexName: String = NameMapping.indexStatistics
  val indexMetaMap: mutable.Map[LabelName, mutable.Set[PropertyName]] = mutable.Map[LabelName, mutable.Set[PropertyName]]()

  val propertyColumnName = NameMapping.indexMetaPropertyName
  val labelColumnName = NameMapping.indexMetaLabelName

  def init(): Unit ={
    if (!indexStore.indexIsExist(indexName)) {
      indexStore.createIndex(indexName)
      initStatistics()
    }
  }

  private def initStatistics(): Unit ={
    val indexMeta = indexStore.loadAllMeta(NameMapping.nodeIndexMeta)
    while (indexMeta.hasNext){
      indexMeta.next().foreach(lineMap => {
        val label = lineMap(labelColumnName).toString
        val propName = lineMap(propertyColumnName).toString
        if (indexMetaMap.contains(label)) indexMetaMap(label).add(propName)
        else indexMetaMap(label) = mutable.Set(propName)
      })
    }
  }
}
