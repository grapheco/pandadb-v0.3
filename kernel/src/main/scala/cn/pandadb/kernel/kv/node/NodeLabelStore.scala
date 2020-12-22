package cn.pandadb.kernel.kv.node

import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.rocksdb.{ReadOptions, RocksDB}

import scala.collection.mutable


class NodeLabelStore(db: RocksDB)  {
  // [nodeId,labelId] -> []

  def set(nodeId: Long, labelId: Int): Unit =
    db.put(KeyHandler.nodeLabelToBytes(nodeId, labelId), Array.emptyByteArray)

  def set(nodeId: Long, labels: Array[Int]): Unit = labels.foreach(set(nodeId, _))

  def delete(nodeId: Long, labelId: Int): Unit =
    db.delete(KeyHandler.nodeLabelToBytes(nodeId, labelId))

  def delete(nodeId: Long): Unit =
    db.deleteRange(KeyHandler.nodeLabelToBytes(nodeId, 0),
      KeyHandler.nodeLabelToBytes(nodeId, -1))

  // return labelIds
  def get(id: Long): Array[Int] = {
    val keyPrefix = KeyHandler.nodeLabelPrefix(id)
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    new Iterator[Int] (){
      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): Int = {
        val label = ByteUtils.getInt(iter.key(), keyPrefix.length)
        iter.next()
        label
      }
    }.toArray
  }

//  def all() : Iterator[(Long, Array[Int])] = {
//    val keyPrefix = KeyHandler.nodeKeyPrefix()
//    val readOptions = new ReadOptions()
//    readOptions.setPrefixSameAsStart(true)
//    readOptions.setTotalOrderSeek(true)
//    val iter = db.newIterator(readOptions)
//    iter.seek(keyPrefix)
//
//    new Iterator[(Long, Array[Int])] (){
//      override def hasNext: Boolean = iter.isValid && iter.key().startsWith(keyPrefix)
//
//      override def next(): (Long, Array[Int]) = {
//        val ret = (ByteUtils.getLong(iter.key(), keyPrefix.length), labelIdsFromBytes(iter.value()))
//        iter.next()
//        ret
//      }
//    }
//
//  }

//  def addLabel(nodeId: Long, labelId: Int): Unit = {
//    val labelIds = this.get(nodeId)
//    val newLabels = mutable.Set[Int]()
//    labelIds.foreach(e => newLabels.add(e))
//    newLabels.add(labelId)
//    this.set(nodeId, newLabels.toArray[Int])
//  }
//
//  def removeLabel(nodeId: Long, labelId: Int): Unit = {
//    val labelIds = this.get(nodeId)
//    val newLabels = mutable.Set[Int]()
//    labelIds.foreach(e => if (e != labelId) newLabels.add(e))
//    this.set(nodeId, newLabels.toArray[Int])
//  }

}
