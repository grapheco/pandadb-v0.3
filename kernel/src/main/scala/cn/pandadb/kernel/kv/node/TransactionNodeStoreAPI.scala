package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.KeyConverter
import cn.pandadb.kernel.kv.meta.{IdGenerator, NodeLabelNameStore, PropertyNameStore, TransactionIdGenerator, TransactionNodeLabelNameStore, TransactionPropertyNameStore}
import cn.pandadb.kernel.store.{StoredNodeWithProperty, TransactionNodeStoreSPI}
import cn.pandadb.kernel.transaction.{DBNameMap, PandaTransaction}
import cn.pandadb.kernel.util.log.LogWriter
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

  override def allLabels(tx: LynxTransaction): Array[String] = nodeLabelName.mapString2Int.keys.toArray

  override def allLabelIds(tx: LynxTransaction): Array[Int] = nodeLabelName.mapInt2String.keys.toArray

  override def getLabelName(labelId: Int): Option[String] = nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Option[Int] = nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String], tx: LynxTransaction, logWriter: LogWriter): Set[Int] = nodeLabelName.ids(labelNames, tx, logWriter)

  override def addLabel(labelName: String, tx: LynxTransaction, logWriter: LogWriter): Int = nodeLabelName.getOrAddId(labelName, tx, logWriter)

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyName.id(keyName)

  override def addPropertyKey(keyName: String, tx: LynxTransaction, logWriter: LogWriter): Int = propertyName.getOrAddId(keyName, tx, logWriter)

  override def getNodeById(nodeId: Long, tx: LynxTransaction): Option[StoredNodeWithProperty] = {
    nodeLabelStore.get(nodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
      .map(nodeStore.get(nodeId, _, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB)).get)
  }

  override def getNodeById(nodeId: Long, label: Int, tx: LynxTransaction): Option[StoredNodeWithProperty] = {
    nodeStore.get(nodeId, label, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
  }

  override def getNodeById(nodeId: Long, label: Option[Int], tx: LynxTransaction): Option[StoredNodeWithProperty] = {
    label.map(getNodeById(nodeId, _, tx))
      .getOrElse(getNodeById(nodeId, tx))
  }

  override def getNodesByLabel(labelId: Int, tx: LynxTransaction): Iterator[StoredNodeWithProperty] =
    nodeStore.getNodesByLabel(labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))

  override def getNodeIdsByLabel(labelId: Int, tx: LynxTransaction): Iterator[Long] =
    nodeStore.getNodeIdsByLabel(labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))

  override def getNodeLabelsById(nodeId: Long, tx: LynxTransaction): Array[Int] =
    nodeLabelStore.getAll(nodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))

  override def hasLabel(nodeId: Long, label: Int, tx: LynxTransaction): Boolean =
    nodeLabelStore.exist(nodeId, label, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))

  override def newNodeId(): Long = idGenerator.nextId()

  override def nodeAddLabel(nodeId: Long, labelId: Int, tx: LynxTransaction, logWriter: LogWriter): Unit = {
    getNodeById(nodeId, tx)
      .foreach { node =>
        if (!node.labelIds.contains(labelId)) {
          val labels = node.labelIds ++ Array(labelId)
          nodeLabelStore.set(nodeId, labelId, tx, logWriter)
          nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties), tx, logWriter)
          // if node is nonLabel node, delete it
          if (node.labelIds.isEmpty) {
            nodeLabelStore.delete(nodeId, NONE_LABEL_ID, tx, logWriter)
            nodeStore.delete(nodeId, NONE_LABEL_ID, tx, logWriter)
          }
        }
      }
  }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int, tx: LynxTransaction, logWriter: LogWriter): Unit = {
    nodeStore.get(nodeId, labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
      .foreach { node =>
        if (node.labelIds.contains(labelId)) {
          val labels = node.labelIds.filter(_ != labelId)
          val newNode = new StoredNodeWithProperty(node.id, labels, node.properties)
          // if node is only one label, add NONE_LABEL_ID after delete it
          if (node.labelIds.length == 1) {
            nodeLabelStore.set(nodeId, NONE_LABEL_ID, tx, logWriter)
            nodeStore.set(NONE_LABEL_ID, newNode, tx, logWriter)
          }
          nodeLabelStore.delete(node.id, labelId, tx, logWriter)
          nodeStore.set(newNode, tx, logWriter)
          nodeStore.delete(nodeId, labelId, tx, logWriter)
        }
      }
  }

  override def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any, tx: LynxTransaction, logWriter: LogWriter): Unit = {
    getNodeById(nodeId, tx)
      .foreach {
        node =>
          nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds,
            node.properties ++ Map(propertyKeyId -> propertyValue)), tx, logWriter)
      }
  }

  override def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int, tx: LynxTransaction, logWriter: LogWriter): Any = {
    getNodeById(nodeId, tx)
      .foreach {
        node =>
          nodeStore.set(new StoredNodeWithProperty(node.id, node.labelIds,
            node.properties - propertyKeyId), tx, logWriter)
      }
  }

  override def addNode(node: StoredNodeWithProperty, tx: LynxTransaction, logWriter: LogWriter): Unit = {
    if (node.labelIds != null && node.labelIds.length > 0) {
      nodeStore.set(node, tx, logWriter)
      nodeLabelStore.set(node.id, node.labelIds, tx, logWriter)
    }
    else {
      nodeStore.set(NONE_LABEL_ID, node, tx, logWriter)
      nodeLabelStore.set(node.id, NONE_LABEL_ID, tx, logWriter)
    }
  }

  override def allNodes(tx: LynxTransaction): Iterator[StoredNodeWithProperty] = nodeStore.all(tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))

  override def nodesCount(tx: LynxTransaction): Long = nodeLabelStore.getNodesCount(tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))

  override def deleteNode(nodeId: Long, tx: LynxTransaction, logWriter: LogWriter): Unit = {
    nodeLabelStore.getAll(nodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
      .foreach(nodeStore.delete(nodeId, _, tx, logWriter))
    nodeLabelStore.delete(nodeId, tx, logWriter)
  }

  override def deleteNodes(nodeIDs: Iterator[Long], tx: LynxTransaction, logWriter: LogWriter): Unit = {
    val ptx = tx.asInstanceOf[PandaTransaction]

    val nodesWB = new WriteBatch()
    val labelWB = new WriteBatch()
    nodeIDs.foreach(nid => {
      nodeLabelStore.getAllForLog(nid, ptx.rocksTxMap(DBNameMap.nodeLabelDB)).foreach(key => {
        logWriter.writeUndoLog(ptx.id, DBNameMap.nodeLabelDB, key, null)
      })
      nodeLabelStore.getAll(nid, ptx.rocksTxMap(DBNameMap.nodeLabelDB)).foreach(lid => {
        nodeStore.getNodesByLabelForLog(lid,ptx.rocksTxMap(DBNameMap.nodeDB)).foreach(kv => {
          logWriter.writeUndoLog(ptx.id, DBNameMap.nodeDB, kv._1, kv._2)
        })
      })

      nodeLabelStore.getAll(nid, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB)).foreach(lid => {
        nodesWB.delete(KeyConverter.toNodeKey(lid, nid))
      })
      labelWB.deleteRange(KeyConverter.toNodeLabelKey(nid, 0),
        KeyConverter.toNodeLabelKey(nid, -1))
    })

    ptx.rocksTxMap(DBNameMap.nodeDB).rebuildFromWriteBatch(nodesWB)
    ptx.rocksTxMap(DBNameMap.nodeLabelDB).rebuildFromWriteBatch(labelWB)
  }

  override def deleteNodesByLabel(labelId: Int, tx: LynxTransaction, logWriter: LogWriter): Unit = {
    nodeStore.getNodeIdsByLabel(labelId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeDB))
      .foreach {
        nodeId =>
          nodeLabelStore.getAll(nodeId, tx.asInstanceOf[PandaTransaction].rocksTxMap(DBNameMap.nodeLabelDB))
            .foreach {
              nodeStore.delete(nodeId, _, tx, logWriter)
            }
          nodeLabelStore.delete(nodeId, tx, logWriter)
      }
    nodeStore.deleteByLabel(labelId, tx, logWriter)
  }

  override def close(): Unit = {
    nodeDB.close()
    nodeLabelDB.close()
    metaDB.close()
  }
}
