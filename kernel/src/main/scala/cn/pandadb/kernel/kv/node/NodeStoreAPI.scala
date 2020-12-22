package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.NodeId
import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.store.{NodeStoreSPI, StoredNodeWithProperty}

class NodeStoreAPI(dbPath: String){

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeStore = new NodeStore(nodeDB)
  private val nodeLabelDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabel")
  private val nodeLabelStore = new NodeLabelStore(nodeLabelDB)

  val NONE_LABEL_ID: Int = -1

  def addNode(node: StoredNodeWithProperty): Unit = {
    if (node.labelIds!=null && node.labelIds.length>0) {
      nodeStore.set(node)
      nodeLabelStore.set(node.id, node.labelIds)
    }
    else {
      nodeStore.set(NONE_LABEL_ID, node)
      nodeLabelStore.set(node.id, NONE_LABEL_ID)
    }
  }

  def getNode(nodeId: Long): StoredNodeWithProperty = {
    val labels = nodeLabelStore.get(nodeId)
    if (labels.length>0)
      nodeStore.get(nodeId, (0))
    else
      null
  }

  def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] = nodeStore.getNodesByLabel(labelId)

  def getNodeIdsByLabel(labelId: Int): Iterator[Long] = nodeStore.getNodeIdsByLabel(labelId)

  def deleteNode(nodeId: Long): Unit = {
    val labels = nodeLabelStore.get(nodeId)
    nodeStore.delete(nodeId, labels)
    nodeLabelStore.delete(nodeId)
  }

  def deleteNodesByLabel(labelId: Int): Unit = {
    val ids = nodeStore.getNodeIdsByLabel(labelId)
    ids.foreach(nodeLabelStore.delete(_,labelId))
    nodeStore.deleteByLabel(labelId)
  }

  // Big cost!!!
  def addLabelForNode(nodeId: Long, labelId: Int): Unit = {
    val node = getNode(nodeId)
    val labels = node.labelIds ++ Array(labelId)
    nodeLabelStore.set(nodeId, labelId)
    nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
  }

  // Big big cost!!!
  def removeLabelFromNode(nodeId: Long, labelId: Int): Unit = {
    val node = getNode(nodeId)
    val labels = node.labelIds.filter(_!=labelId)
    nodeLabelStore.delete(nodeId, labelId)
    nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
    nodeStore.delete(nodeId, labelId)
  }

  def close(): Unit ={
    nodeDB.close()
    nodeLabelDB.close()
  }

}
