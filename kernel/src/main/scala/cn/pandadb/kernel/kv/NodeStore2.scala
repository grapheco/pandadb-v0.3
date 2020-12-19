package cn.pandadb.kernel.kv

import cn.pandadb.kernel.util.BaseSerializer
import org.rocksdb.{ReadOptions, RocksDB}

import scala.collection.mutable


class NodeStore2(db: RocksDB)  {
  // [type,nodeId]->[labelIds]


  private def labelIdsToBytes(labelIds: Array[Int]): Array[Byte] = {
    BaseSerializer.intArray2Bytes(labelIds)
  }

  private def labelIdsFromBytes(bytes: Array[Byte]): Array[Int] = {
    BaseSerializer.bytes2IntArray(bytes)
  }

  def set(id: Long, labels: Array[Int]): Unit = {
    val keyBytes = KeyHandler.nodeKeyToBytes(id)
    db.put(keyBytes, labelIdsToBytes(labels))
  }

  def delete(id: Long): Array[Int] = {
    val keyBytes = KeyHandler.nodeKeyToBytes(id)
    val valueBytes = db.get(keyBytes)
    db.delete(keyBytes)
    labelIdsFromBytes(valueBytes)
  }

  // return labelIds
  def get(id: Long): Array[Int] = {
    val keyBytes = KeyHandler.nodeKeyToBytes(id)
    val valueBytes = db.get(keyBytes)
    labelIdsFromBytes(valueBytes)
  }

  def all() : Iterator[(Long, Array[Int])] = {
    val keyPrefix = KeyHandler.nodeKeyPrefix()
    val readOptions = new ReadOptions()
    readOptions.setPrefixSameAsStart(true)
    readOptions.setTotalOrderSeek(true)
    val iter = db.newIterator(readOptions)
    iter.seek(keyPrefix)

    new Iterator[(Long, Array[Int])] (){
      override def hasNext: Boolean = iter.isValid() && iter.key().startsWith(keyPrefix)

      override def next(): (Long, Array[Int]) = {
        val ret = (ByteUtils.getLong(iter.key(), keyPrefix.length), labelIdsFromBytes(iter.value()))
        iter.next()
        ret
      }
    }

  }

  def addLabel(nodeId: Long, labelId: Int): Unit = {
    val labelIds = this.get(nodeId)
    val newLabels = mutable.Set[Int]()
    labelIds.foreach(e => newLabels.add(e))
    newLabels.add(labelId)
    this.set(nodeId, newLabels.toArray[Int])
  }

  def removeLabel(nodeId: Long, labelId: Int): Unit = {
    val labelIds = this.get(nodeId)
    val newLabels = mutable.Set[Int]()
    labelIds.foreach(e => if (e != labelId) newLabels.add(e))
    this.set(nodeId, newLabels.toArray[Int])
  }

}
