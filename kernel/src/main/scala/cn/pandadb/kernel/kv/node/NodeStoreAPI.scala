package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.store.StoredNodeWithProperty

class NodeStoreAPI(dbPath: String) {

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeStore = new NodeStore2(nodeDB)
  private val nodeLabelIndexDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabelIndex")
  private val nodeLabelIndexStore = new NodeLabelIndex2(nodeLabelIndexDB)


  val NONE_LABEL_ID = 0

  private def propertiesToBytes(obj: Any): Array[Byte] = {
    Array[Byte]()
  }


  def addNode(node: StoredNodeWithProperty): Unit = {
    val propsBytes = propertiesToBytes(node.properties)
    if (node.labelIds!=null && node.labelIds.length>0) {
      nodeStore.set(node.id, node.labelIds)
      node.labelIds.foreach(labelId => nodeLabelIndexStore.set(labelId, node.id, propsBytes))
//      nodeLabelIndexStore.delete(NONE_LABEL_ID, node.id)
    }
    else {
      nodeStore.set(node.id, Array[Int](NONE_LABEL_ID))
      nodeLabelIndexStore.set(NONE_LABEL_ID, node.id, propsBytes)
    }
  }

  def getNode(nodeId: Long): StoredNodeWithProperty = {
//    val labelIds = nodeStore.get(nodeId)
//    if (labelIds == null) {
//      null
//    }
//    else if (labelIds.length == 1 && labelIds.length.equals(NONE_LABEL_ID)) { // none label
//      val propsBytes: Array[Byte] = nodeLabelIndexStore.get(NONE_LABEL_ID, nodeId)
//      new StoredNodeWithProperty(nodeId, null, propertiesToBytes(propsBytes))
//    }
    null
  }

}
