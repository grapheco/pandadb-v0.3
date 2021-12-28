package cn.pandadb.kernel.distribute.index

import java.util.concurrent.atomic.AtomicInteger

import cn.pandadb.kernel.distribute.DistributedKVAPI
import cn.pandadb.kernel.distribute.meta.{NodeLabelNameStore, PropertyNameStore}
import cn.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.kernel.util.PandaDBException.PandaDBException

import scala.collection.mutable

trait IndexNameStore {
  val db: DistributedKVAPI
  val keyPrefixFunc: () => Array[Byte]
  val keyWithLabelPrefixFunc: (Int) => Array[Byte]
  val keyWithIndexFunc: (Int, Int) => Array[Byte]

  val nodeStore: DistributedNodeStoreSPI

  // a label with a set of properties
  var indexMetaMap: mutable.Map[String, mutable.Set[String]] = mutable.Map[String, mutable.Set[String]]()

  def getIndexedMeta(): Map[String, Seq[String]] ={
    indexMetaMap.map(f => (f._1, f._2.toSeq)).toMap
  }

  def addToDB(labelName: String, propertyName: String): Unit = {

    if (indexMetaMap.contains(labelName) && indexMetaMap(labelName).contains(propertyName))
      throw new PandaDBException(s"already has index on: LABEL: $labelName with PROPERTY: $propertyName")

    if (indexMetaMap.contains(labelName)) indexMetaMap(labelName).add(propertyName)
    else indexMetaMap += labelName -> mutable.Set(propertyName)

    val labelId = nodeStore.getLabelId(labelName)
    val propertyId = nodeStore.getPropertyKeyId(propertyName)
    if (labelId.isDefined && propertyId.isDefined){
      val key = keyWithIndexFunc(labelId.get, propertyId.get)
      db.put(key, Array.emptyByteArray)
    }
    else throw new PandaDBException("no such label or property to create index")
  }

  def delete(labelName: String, propertyName: String): Unit = {
    val id = nodeStore.getLabelId(labelName)
    val pid = nodeStore.getPropertyKeyId(propertyName)
    if (id.isDefined && pid.isDefined){
      val key = keyWithIndexFunc(id.get, pid.get)
      db.delete(key)

      indexMetaMap(labelName).remove(propertyName)
      if (indexMetaMap(labelName).isEmpty) indexMetaMap -= labelName
    }
  }

  def loadAll(): Unit = {
    val prefix = keyPrefixFunc()
    val iter = db.scanPrefix(prefix, 10000, true)
    while (iter.hasNext){
      val key = iter.next().getKey.toByteArray
      val label = nodeStore.getLabelName(ByteUtils.getInt(key, 1)).get
      val property = nodeStore.getPropertyKeyName(ByteUtils.getInt(key, 5)).get
      if (indexMetaMap.contains(label)) indexMetaMap(label).add(property)
      else indexMetaMap += label -> mutable.Set(property)
    }
  }
}
