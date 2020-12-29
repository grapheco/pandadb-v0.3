package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.RocksDBStorage
import cn.pandadb.kernel.kv.meta.{NodeLabelNameStore, PropertyNameStore}
import cn.pandadb.kernel.store.{NodeStoreSPI, StoredNodeWithProperty}

/**
 * TODO
 */

class NodeStoreAPI(dbPath: String) extends NodeStoreSPI {

  private val nodeDB = RocksDBStorage.getDB(s"${dbPath}/nodes")
  private val nodeStore = new NodeStore(nodeDB)
  private val nodeLabelDB = RocksDBStorage.getDB(s"${dbPath}/nodeLabel")
  private val nodeLabelStore = new NodeLabelStore(nodeLabelDB)
  private val metaDB = RocksDBStorage.getDB(s"${dbPath}/nodeMeta")
  private val nodeLabelName = new NodeLabelNameStore(metaDB)
  private val propertyName = new PropertyNameStore(metaDB)

  val NONE_LABEL_ID: Int = -1

  override def allLabels(): Array[String] = nodeLabelName.mapString2Int.keys.toArray

  override def allLabelIds(): Array[Int] = nodeLabelName.mapInt2String.keys.toArray

  override def getLabelName(labelId: Int): Option[String] = nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Int = nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String]): Set[Int] = nodeLabelName.ids(labelNames)

  override def addLabel(labelName: String): Int = nodeLabelName.id(labelName)

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Int = propertyName.id(keyName)

  override def addPropertyKey(keyName: String): Int = propertyName.id(keyName)

  override def getNodeById(nodeId: Long): Option[StoredNodeWithProperty] =
    nodeLabelStore.get(nodeId).map(nodeStore.get(nodeId, _).get)

  override def nodeAddLabel(nodeId: Long, labelId: Int): Unit =
    getNodeById(nodeId)
      .foreach{ node =>
        val labels = node.labelIds ++ Array(labelId)
        nodeLabelStore.set(nodeId, labelId)
        nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
      }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit =
    nodeStore.get(nodeId, labelId)
      .foreach{ node=>
        val labels = node.labelIds.filter(_ != labelId)
        nodeLabelStore.delete(node.id, labelId)
        nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
        nodeStore.delete(nodeId, labelId)
      }

  override def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit =
    getNodeById(nodeId)
      .foreach{
        node =>
          nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds,
            node.properties ++ Map(propertyKeyId -> propertyValue)))
      }


  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any = {
    getNodeById(nodeId)
      .foreach{
        node =>
          nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds,
            node.properties-propertyKeyId))
      }
  }

  override def addNode(node: StoredNodeWithProperty): Unit = {
    if (node.labelIds!=null && node.labelIds.length>0) {
      nodeStore.set(node)
      nodeLabelStore.set(node.id, node.labelIds)
    }
    else {
      nodeStore.set(NONE_LABEL_ID, node)
      nodeLabelStore.set(node.id, NONE_LABEL_ID)
    }
  }

  override def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  override def nodesCount: Long = nodeLabelStore.getNodesCount

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] = nodeStore.getNodesByLabel(labelId)

  override def getNodeIdsByLabel(labelId: Int): Iterator[Long] = nodeStore.getNodeIdsByLabel(labelId)

  override def deleteNode(nodeId: Long): Unit = {
    nodeLabelStore.getAll(nodeId)
      .foreach(nodeStore.delete(nodeId, _))
    nodeLabelStore.delete(nodeId)
  }

  // big cost
  override def deleteNodesByLabel(labelId: Int): Unit = {
    nodeStore.getNodeIdsByLabel(labelId)
      .foreach{
        nodeid=>
          nodeLabelStore.getAll(nodeid)
            .foreach{
              nodeStore.delete(nodeid, _)
            }
          nodeLabelStore.delete(nodeid)
    }
    nodeStore.deleteByLabel(labelId)
  }

  //  // Big cost!!!
  //  def addLabelForNode(nodeId: Long, labelId: Int): Unit = {
  //    val node = getNode(nodeId)
  //    val labels = node.labelIds ++ Array(labelId)
  //    nodeLabelStore.set(nodeId, labelId)
  //    nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
  //  }
  //
  //  // Big big cost!!!
  //  def removeLabelFromNode(nodeId: Long, labelId: Int): Unit = {
  //    val node = getNode(nodeId)
  //    val labels = node.labelIds.filter(_!=labelId)
  //    nodeLabelStore.delete(nodeId, labelId)
  //    nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
  //    nodeStore.delete(nodeId, labelId)
  //  }

  override def close(): Unit ={
    nodeDB.close()
    nodeLabelDB.close()
    metaDB.close()
  }

}
