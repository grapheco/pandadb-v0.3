package org.grapheco.pandadb.kernel.distribute.index

import org.grapheco.lynx.{Index, NodeFilter}
import org.grapheco.pandadb.kernel.distribute.index.utils.IndexTool
import org.grapheco.pandadb.kernel.store.{IndexNode, PandaNode}

trait IndexStoreService {
  def cleanIndexes(indexName: String*): Unit
  def getIndexTool: IndexTool

  //meta
  def refreshIndexMeta(): Unit
  def getIndexMeta: Map[String, Seq[String]]

  // encoding
  def getEncodingMeta(): Map[String, Array[Byte]] // for encoding like tree
  def removeEncodingMeta(name: String): Unit
  def setEncodingMeta(name: String, value: Array[Byte]): Unit

  def getNodeByDocId(docId: String): IndexNode
  def deleteNodeByNodeId(nodeId: String): Unit
  def searchNodes(labels: Seq[String], filter: Map[String, Any]): Iterator[Seq[Long]]

  def isNodeHasIndex(filter: NodeFilter): Boolean
  def isIndexCreated(targetLabel: String, propNames: Seq[String]): Boolean
  def isLabelHasEncoder(label: String): Boolean

  def setIndexOnSingleNode(nodeId: Long, labels: Seq[String], nodeProps: Map[String, Any]): Unit
  def dropIndexOnSingleNodeProp(nodeId: Long, labels: Seq[String], dropPropName: String): Unit
  def dropIndexOnSingleNodeLabel(nodeId: Long, label: String): Unit

  def batchAddIndexOnNodes(targetLabel: String, targetPropNames: Seq[String], nodes: Iterator[PandaNode]): Unit
  def batchDropEncoder(label: String, encoderName: String, nodes: Iterator[PandaNode]): Unit
  def batchDropIndexLabelWithProperty(indexLabel: String, targetPropName: String, nodes: Iterator[PandaNode]): Unit

  def fullTextSearch(columnName: Seq[String], text: String): Iterator[Long]
}
