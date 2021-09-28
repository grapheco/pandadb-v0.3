package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.KeyConverter.NodeId
import cn.pandadb.kernel.kv.{KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.kv.meta.{IdGenerator, NodeLabelNameStore, PropertyNameStore}
import cn.pandadb.kernel.store.{NodeStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.DBNameMap
import org.rocksdb.{WriteBatch, WriteOptions}


class NodeStoreAPI(nodeDBPath: String, nodeDBConfigPath: String,
                   nodeLabelDBPath: String, nodeLabelConfigPath: String,
                   metaDBPath: String, metaDBConfigPath: String) extends NodeStoreSPI {

  private val nodeDB = RocksDBStorage.getDB(nodeDBPath, rocksdbConfigPath = nodeDBConfigPath)
  private val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)
  private val nodeStore = new NodeStore(nodeDB)
  private val nodeLabelDB = RocksDBStorage.getDB(nodeLabelDBPath, rocksdbConfigPath = nodeLabelConfigPath)
  private val nodeLabelStore = new NodeLabelStore(nodeLabelDB)
  private val metaDB = RocksDBStorage.getDB(metaDBPath, rocksdbConfigPath = metaDBConfigPath)
  private val nodeLabelName = new NodeLabelNameStore(metaDB)
  private val propertyName = new PropertyNameStore(metaDB)
  private val idGenerator = new IdGenerator(nodeLabelDB, 200)

  val NONE_LABEL_ID: Int = 0

  def this(dbPath: String, rocksdbCfgPath: String = "default"){
    this(s"${dbPath}/${DBNameMap.nodeDB}", rocksdbCfgPath, s"${dbPath}/${DBNameMap.nodeLabelDB}", rocksdbCfgPath, s"${dbPath}/${DBNameMap.nodeMetaDB}", rocksdbCfgPath)
  }

  override def allLabels(): Array[String] = nodeLabelName.mapString2Int.keys.toArray

  override def allLabelIds(): Array[Int] = nodeLabelName.mapInt2String.keys.toArray

  override def getLabelName(labelId: Int): Option[String] = nodeLabelName.key(labelId)

  override def getLabelId(labelName: String): Option[Int] = nodeLabelName.id(labelName)

  override def getLabelIds(labelNames: Set[String]): Set[Int] = nodeLabelName.ids(labelNames)

  override def addLabel(labelName: String): Int = nodeLabelName.getOrAddId(labelName)

  override def allPropertyKeys(): Array[String] = propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] = propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] = propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] = propertyName.id(keyName)

  override def addPropertyKey(keyName: String): Int = propertyName.getOrAddId(keyName)

  override def getNodeById(nodeId: Long): Option[StoredNodeWithProperty] ={
//    val labelId = nodeLabelStore.get(nodeId).get
//    nodeStore.get(nodeId, labelId)
    nodeLabelStore.get(nodeId).map(nodeStore.get(nodeId, _).get)
  }

  override def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty] =
    nodeStore.get(nodeId, label)


  override def getNodeLabelsById(nodeId: Long): Array[Int] = nodeLabelStore.getAll(nodeId)

  override def hasLabel(nodeId: Long, label: Int): Boolean = nodeLabelStore.exist(nodeId, label)

  override def nodeAddLabel(nodeId: Long, labelId: Int): Unit =
    getNodeById(nodeId)
      .foreach{ node =>
        if (!node.labelIds.contains(labelId)) {
          val labels = node.labelIds ++ Array(labelId)
          nodeLabelStore.set(nodeId, labelId)
          nodeStore.set(new StoredNodeWithProperty(node.id, labels, node.properties))
          // if node is nonLabel node, delete it
          if(node.labelIds.isEmpty){
            nodeLabelStore.delete(nodeId, NONE_LABEL_ID)
            nodeStore.delete(nodeId, NONE_LABEL_ID)
          }
        }
      }

  override def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit =
    nodeStore.get(nodeId, labelId)
      .foreach{ node=>
        if (node.labelIds.contains(labelId)) {
          val labels = node.labelIds.filter(_ != labelId)
          val newNode = new StoredNodeWithProperty(node.id, labels, node.properties)
          // if node is only one label, add NONE_LABEL_ID after delete it
          if(node.labelIds.length == 1){
            nodeLabelStore.set(nodeId, NONE_LABEL_ID)
            nodeStore.set(NONE_LABEL_ID, newNode)
          }
          nodeLabelStore.delete(node.id, labelId)
          nodeStore.set(newNode)
          nodeStore.delete(nodeId, labelId)
        }
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
  def all2(): Iterator[StoredNodeWithProperty]  ={
    nodeStore.all2()
  }
  override def allNodes(): Iterator[StoredNodeWithProperty] = nodeStore.all()

  override def nodesCount: Long = nodeLabelStore.getNodesCount

  override def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty] = nodeStore.getNodesByLabel(labelId)

  override def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty] =
    label.map(getNodeById(nodeId, _)).getOrElse(getNodeById(nodeId))

  override def getNodeIdsByLabel(labelId: Int): Iterator[Long] = nodeStore.getNodeIdsByLabel(labelId)

  override def deleteNode(nodeId: Long): Unit = {
    nodeLabelStore.getAll(nodeId)
      .foreach(nodeStore.delete(nodeId, _))
    nodeLabelStore.delete(nodeId)
  }

  override def deleteNodes(nodeIDs: Iterator[NodeId]): Unit = {
    val nodesWB = new WriteBatch()
    val labelWB = new WriteBatch()
    nodeIDs.foreach(nid => {
      nodeLabelStore.getAll(nid).foreach(lid => {
        nodesWB.delete(KeyConverter.toNodeKey(lid, nid))
      })
      labelWB.deleteRange(KeyConverter.toNodeLabelKey(nid, 0),
        KeyConverter.toNodeLabelKey(nid, -1))
    })
    nodeDB.write(writeOptions, nodesWB) //TODO Important! to guarantee atomic
    nodeLabelDB.write(writeOptions, labelWB) //TODO Important! to guarantee atomic
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

  override def close(): Unit ={
    nodeDB.close()
    nodeLabelDB.close()
    metaDB.close()
  }

  override def newNodeId(): Long = {
    idGenerator.nextId()
  }
}
