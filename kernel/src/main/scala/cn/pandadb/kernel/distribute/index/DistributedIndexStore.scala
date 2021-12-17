//package cn.pandadb.kernel.distribute.index
//
//import cn.pandadb.kernel.distribute.meta.NameMapping
//import org.elasticsearch.action.bulk.BulkProcessor
//import org.elasticsearch.ingest.Processor
//
//trait DistributedIndexStore {
//  type NodeID = Long
//  type NodeLabel = String
//  type NodeLabels = List[String]
//  type NodePropertyName = String
//  type NodePropertyValue = Any
//
//  def serviceIsAvailable(): Boolean
//
//  // =========================================es index============================
//  def indexIsExist(indexNames: String*): Boolean
//
//  def cleanIndexes(indexNames: String*): Unit
//
//  def createIndex(indexName: String, extraSettings: Map[String, Any]): Boolean
//
//  def deleteIndex(indexName: String): Boolean
//
//  // ==========================================common======================================
//  def deleteDoc(indexName: String, docId: String): Unit
//
//  def searchDoc(filter: Seq[(String, Any)], indexName: String = NameMapping.nodeIndex): Iterator[Seq[String]]
//
//  def docExist(indexName: String, docId: String): Boolean
//
//  def addDoc(indexName: String, docId: Option[String], dataMap: Map[String, Any]): Unit
//
//  // ===========================================db index===================================================================
//  def createIndexField(indexName: String, header: (NodeLabel, NodePropertyName), data: Iterator[(NodeID, NodeLabels, NodePropertyValue)])
//
//  def updateIndexField(indexName: String, header: (NodeLabel, NodeProperty), data: Iterator[(NodeID, NodeLabel, NodeProperty, NodePropertyValue)])
//
//  def deleteIndexField(indexName: String, label: String, propertyName: String)
//  // ======================================================================================================================
//
//  // statistics
//  def getHits(filter: Seq[(String, Any)], indexName: String): Long
//}
