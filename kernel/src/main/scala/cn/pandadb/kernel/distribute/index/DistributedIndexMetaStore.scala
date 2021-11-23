package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.NameMapping

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-23 15:30
 */
trait DistributedIndexMetaStore {
  val indexStore: DistributedIndexStore
  val indexName: String

  var dataMap: mutable.Map[String, ArrayBuffer[String]] = mutable.Map[String, ArrayBuffer[String]]()

  private def addToIndex(label: String, property: String): Unit = {
    if (dataMap.contains(label)) dataMap(label).append(property)
    else dataMap += label -> ArrayBuffer(property)

    indexStore.addIndexMetaDoc(indexName, label, property)
  }

  def existOrAdd(label: String, property: String): Unit = {
    if (!hasMeta(label, property)) {
      addToIndex(label, property)
    }
  }

  def delete(label: String, property: String): Unit = {
    dataMap(label) -= property
    if (dataMap(label).isEmpty) dataMap -= label
    indexStore.deleteIndexMetaDoc(indexName, label, property)
  }

  def loadAll(): Unit = {
    val iter = indexStore.loadAllMeta(indexName)
    while (iter.hasNext) {
      val res = iter.next().map(kv => (kv(NameMapping.indexMetaLabelName).toString, kv(NameMapping.indexMetaPropertyName).toString))
      res.foreach(lp => {
        if (dataMap.contains(lp._1)) dataMap(lp._1).append(lp._2)
        else dataMap(lp._1) = ArrayBuffer[String](lp._2)
      })
    }
  }

  def hasMeta(label: String, propertyName: String): Boolean = {
    if (dataMap.contains(label)) {
      if (dataMap(label).contains(propertyName)) true
      else false
    }
    else false
  }
}
