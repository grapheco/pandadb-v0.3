package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.NameMapping

trait DistributedIndexStore {
  type StatusResponse = Int

  def serviceIsAvailable(): Boolean

  // =========================================es index============================
  def indexIsExist(indexNames: String*): Boolean

  def cleanIndexes(indexNames: String*): Unit

  def createIndex(indexName: String, extraSettings: Map[String, Any]): Boolean

  def deleteIndex(indexName: String): Boolean

  // ==========================================common======================================
  def deleteDoc(indexName: String, docId: String): Unit

  def searchDoc(filter: Seq[(String, Any)], indexName: String = NameMapping.nodeIndex): Iterator[Seq[String]]

  // ============================doc for name store=========================================
  def addNameMetaDoc(indexName: String, key: String, value: Int): StatusResponse

  def searchNameMetaDoc(indexName: String, key: String): Option[Map[String, Int]]

  def searchNameMetaDoc(indexName: String, id: Int): Option[Map[String, Int]]

  def loadAllMeta(indexName: String): Iterator[Seq[Map[String, AnyRef]]]

  // ===========================================db index===================================================================
  def addIndexMetaDoc(indexName: String, label: String, property: String): Unit

  def deleteIndexMetaDoc(indexName: String, label: String, propertyName: String): Unit

  def addIndexField(_id: Long, label: String, propertyName: String, propValue: Any, indexName: String = NameMapping.nodeIndex)

  def updateIndexField(_id: Long, label: String,  propertyName: String, propValue: Any, indexName: String = NameMapping.nodeIndex)

  def deleteIndexField(label: String, propertyName: String, indexName: String = NameMapping.nodeIndex)
  // ======================================================================================================================
}
