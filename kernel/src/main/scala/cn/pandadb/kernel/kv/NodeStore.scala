package cn.pandadb.kernel.kv

import cn.pandadb.kernel.store.StoredNodeWithProperty
import org.rocksdb.{ReadOptions, RocksDB}

import scala.collection.mutable

class NodeValue(override val id:Long, override val labelIds: Array[Int], override val  properties: Map[String, Any])
  extends StoredNodeWithProperty(id, labelIds, properties ) {
}

object NodeValue {
  def parseFromBytes(bytes: Array[Byte]): NodeValue = {
    val valueMap = ByteUtils.mapFromBytes(bytes)
    val id = valueMap.get("_id").get.asInstanceOf[Long]
    val labels = valueMap.get("_labels").get.asInstanceOf[Array[Int]]
    val props = valueMap.get("_props").get.asInstanceOf[Map[String, Any]]
    new NodeValue(id, labels, props)
  }

  def toBytes(nodeValue: NodeValue): Array[Byte] = {
    ByteUtils.mapToBytes(Map[String,Any]("_id"->nodeValue.id,
                                          "_labels"->nodeValue.labelIds,
                                          "_props"->nodeValue.properties ))
  }

  def toBytes(id: Long, labels: Array[Int], properties: Map[String, Any]): Array[Byte] = {
    ByteUtils.mapToBytes(Map[String,Any]("_id"->id,
      "_labels"->labels,
      "_props"->properties ))
  }
}

class NodeStore(db: RocksDB)  {

  def set(id: Long, labels: Array[Int], properties: Map[String, Any]): Unit = {
    val keyBytes = KeyHandler.nodeKeyToBytes(id)
    db.put(keyBytes, NodeValue.toBytes(id, labels, properties ))
  }

  def delete(id: Long): Unit = {
    val keyBytes = KeyHandler.nodeKeyToBytes(id)
    db.delete(keyBytes)
  }

  def get(id: Long): NodeValue = {
    val keyBytes = KeyHandler.nodeKeyToBytes(id)
    val valueBytes = db.get(keyBytes)
    NodeValue.parseFromBytes(valueBytes)
  }

  def all() : Iterator[NodeValue] = {
    val keyPrefix = KeyHandler.nodeKeyPrefix()
    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = db.newIterator(readOptions)
    iter.seek(keyPrefix)

    new Iterator[NodeValue] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(keyPrefix)

      override def next(): NodeValue = {
        val node = NodeValue.parseFromBytes(iter.value())
        iter.next()
        node
      }
    }

  }

  def addLabel(id: Long, label: Int): Unit = {
    val nodeValue = this.get(id)
    val newLabels = mutable.Set[Int]()
    nodeValue.labelIds.foreach(e => newLabels.add(e))
    newLabels.add(label)
    this.set(id, newLabels.toArray[Int], nodeValue.properties)
  }

  def removeLabel(id: Long, label: Int): Unit = {
    val nodeValue = this.get(id)
    val newLabels = mutable.Set[Int]()
    nodeValue.labelIds.foreach(e => if (e != label) newLabels.add(e))
    this.set(id, newLabels.toArray[Int], nodeValue.properties)
  }

  def setProperty(id: Long, propertyKey: String, propertyValue: Any): Unit = {
    val nodeValue = this.get(id)
    val newProps = new mutable.HashMap[String, Any]()
    if (nodeValue.properties != null) {
      nodeValue.properties.foreach(e => newProps(e._1)=e._2)
    }
    newProps(propertyKey) = propertyValue
    this.set(id, nodeValue.labelIds, newProps.toMap)
  }

  def removeProperty(id: Long, propertyKey: String): Unit = {
    val nodeValue = this.get(id)
    val newProps = new mutable.HashMap[String, Any]()
    if (nodeValue.properties != null) {
      nodeValue.properties.foreach(e => if (e._1 != propertyKey) newProps(e._1)=e._2)
    }
    this.set(id, nodeValue.labelIds, newProps.toMap)
  }

}

trait NodeWriter {
  def deleteNode(nodeId: Long);

  def addNode(nodeId: Long);

  def addLabel(nodeId: Long, label: Label): Unit;

  def removeLabel(nodeId: Long, label: Label): Unit;
}
