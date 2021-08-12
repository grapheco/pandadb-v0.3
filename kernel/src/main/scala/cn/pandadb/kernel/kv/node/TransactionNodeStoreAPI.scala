package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.meta.{IdGenerator, NodeLabelNameStore, PropertyNameStore, TransactionIdGenerator, TransactionNodeLabelNameStore, TransactionPropertyNameStore}
import cn.pandadb.kernel.store.{StoredNodeWithProperty, TransactionNodeStoreSPI}
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import org.grapheco.lynx.LynxTransaction
import org.rocksdb.{Transaction, TransactionDB, WriteBatch, WriteOptions}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 9:48 上午 2021/8/9
 * @Modified By:
 */
class TransactionNodeStoreAPI(nodeDB: TransactionDB,
                              nodeLabelDB: TransactionDB,
                              metaDB: TransactionDB) extends TransactionNodeStoreSPI {
  // only modify the write relevant funcs
  // addNode(data, rocksdbTx) = { tx.execute()}

  private val nodeStore = new TransactionNodeStore(nodeDB)
  private val nodeLabelStore = new TransactionNodeLabelStore(nodeLabelDB)
  private val nodeLabelName = new TransactionNodeLabelNameStore(metaDB)
  private val propertyName = new TransactionPropertyNameStore(metaDB)
  private val idGenerator = new TransactionIdGenerator(nodeLabelDB, 200)

  val NONE_LABEL_ID: Int = 0

  override def generateTransactions(writeOptions: WriteOptions): Map[String, Transaction] = {
    Map(DBNameMap.nodeDB -> nodeDB.beginTransaction(writeOptions),
      DBNameMap.nodeLabelDB -> nodeLabelDB.beginTransaction(writeOptions),
      DBNameMap.nodeMetaDB -> metaDB.beginTransaction(writeOptions))
  }

  override def allLabels(): Array[String] = nodeLabelName.mapString2Int.keys.toArray

  override def allLabelIds(): Array[Int] = nodeLabelName.mapInt2String.keys.toArray

  override def getLabelName(labelId: Int): Option[String] = nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Option[Int] = nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String], tx: LynxTransaction): Set[Int] = nodeLabelName.ids(labelNames, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeMetaDB))

  override def addLabel(labelName: String, tx: LynxTransaction): Int = nodeLabelName.getOrAddId(labelName, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeMetaDB))

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyName.id(keyName)

  override def addPropertyKey(keyName: String, tx: LynxTransaction): Int = propertyName.getOrAddId(keyName, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeMetaDB))

  override def getNodeById(nodeId: Long): Option[StoredNodeWithProperty] = nodeLabelStore.get(nodeId).map(nodeStore.get(nodeId, _).get)

  override def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty] = nodeStore.get(nodeId, label)

  override def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty] = label.map(getNodeById(nodeId, _)).getOrElse(getNodeById(nodeId))

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] = nodeStore.getNodesByLabel(labelId)

  override def getNodeIdsByLabel(labelId: Int): Iterator[Long] = nodeStore.getNodeIdsByLabel(labelId)

  override def getNodeLabelsById(nodeId: Long): Array[Int] = nodeLabelStore.getAll(nodeId)

  override def hasLabel(nodeId: Long, label: Int): Boolean = nodeLabelStore.exist(nodeId, label)

  override def newNodeId(): Long = idGenerator.nextId()

  override def nodeAddLabel(nodeId: Long, labelId: Int, tx: LynxTransaction): Unit = {
    getNodeById(nodeId)
      .foreach { node =>
        if (!node.labelIds.contains(labelId)) {
          val labels = node.labelIds ++ Array(labelId)
          nodeLabelStore.set(nodeId, labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
          nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties), tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
          // if node is nonLabel node, delete it
          if (node.labelIds.isEmpty) {
            nodeLabelStore.delete(nodeId, NONE_LABEL_ID, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
            nodeStore.delete(nodeId, NONE_LABEL_ID, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
          }
        }
      }
  }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int, tx: LynxTransaction): Unit = {
    nodeStore.get(nodeId, labelId)
      .foreach { node =>
        if (node.labelIds.contains(labelId)) {
          val labels = node.labelIds.filter(_ != labelId)
          val newNode = new StoredNodeWithProperty(node.id, labels, node.properties)
          // if node is only one label, add NONE_LABEL_ID after delete it
          if (node.labelIds.length == 1) {
            nodeLabelStore.set(nodeId, NONE_LABEL_ID, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
            nodeStore.set(NONE_LABEL_ID, newNode, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
          }
          nodeLabelStore.delete(node.id, labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
          nodeStore.set(newNode, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
          nodeStore.delete(nodeId, labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
        }
      }
  }

  override def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any, tx: LynxTransaction): Unit = {
    getNodeById(nodeId)
      .foreach {
        node =>
          nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds,
            node.properties ++ Map(propertyKeyId -> propertyValue)), tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
      }
  }

  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int, tx: LynxTransaction): Any = {
    getNodeById(nodeId)
      .foreach {
        node =>
          nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds,
            node.properties - propertyKeyId), tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
      }
  }

  override def addNode(node: StoredNodeWithProperty, tx: LynxTransaction): Unit = {
    if (node.labelIds != null && node.labelIds.length > 0) {
      nodeStore.set(node, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
      nodeLabelStore.set(node.id, node.labelIds, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
    }
    else {
      nodeStore.set(NONE_LABEL_ID, node, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
      nodeLabelStore.set(node.id, NONE_LABEL_ID, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
    }
  }

  override def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  override def nodesCount: Long = nodeLabelStore.getNodesCount

  override def deleteNode(nodeId: Long, tx: LynxTransaction): Unit = {
    nodeLabelStore.getAll(nodeId)
      .foreach(nodeStore.delete(nodeId, _, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB)))
    nodeLabelStore.delete(nodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
  }

  override def deleteNodes(nodeIDs: Iterator[Long], tx: LynxTransaction): Unit = {
    val nodesWB = new WriteBatch()
    val labelWB = new WriteBatch()
    nodeIDs.foreach(nid => {
      nodeLabelStore.getAll(nid).foreach(lid => {
        nodesWB.delete(KeyConverter.toNodeKey(lid, nid))
      })
      labelWB.deleteRange(KeyConverter.toNodeLabelKey(nid, 0),
        KeyConverter.toNodeLabelKey(nid, -1))
    })

    tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB).rebuildFromWriteBatch(nodesWB)
    tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB).rebuildFromWriteBatch(labelWB)
  }

  override def deleteNodesByLabel(labelId: Int, tx: LynxTransaction): Unit = {
    nodeStore.getNodeIdsByLabel(labelId)
      .foreach {
        nodeId =>
          nodeLabelStore.getAll(nodeId)
            .foreach {
              nodeStore.delete(nodeId, _, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
            }
          nodeLabelStore.delete(nodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
      }
    nodeStore.deleteByLabel(labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
  }

  override def close(): Unit = {
    nodeDB.close()
    nodeLabelDB.close()
    metaDB.close()
  }
}
