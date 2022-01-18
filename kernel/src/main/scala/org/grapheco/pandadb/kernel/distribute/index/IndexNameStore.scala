package org.grapheco.pandadb.kernel.distribute.index

import java.util.concurrent.atomic.AtomicInteger

import org.grapheco.pandadb.kernel.distribute.{DistributedKVAPI, DistributedKeyConverter}
import org.grapheco.pandadb.kernel.distribute.meta.{NodeLabelNameStore, PropertyNameStore}
import org.grapheco.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import org.grapheco.pandadb.kernel.kv.ByteUtils
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException

import scala.collection.mutable

trait IndexNameStore {
  val db: DistributedKVAPI
  val keyPrefixFunc: () => Array[Byte]
  val encodingKeyPrefix: () => Array[Byte]

  val keyWithLabelPrefixFunc: (Int) => Array[Byte]
  val keyWithIndexFunc: (Int, Int) => Array[Byte]

  val nodeStore: DistributedNodeStoreSPI
  val udpClientManager: UDPClientManager


  // a label with a set of properties
  var indexMetaMap: mutable.Map[String, mutable.Set[String]] = mutable.Map[String, mutable.Set[String]]()

  var encodingMetaMap: mutable.Map[String, Array[Byte]] = mutable.Map[String, Array[Byte]]()
  def getEncodingMetaMap = encodingMetaMap.toMap

  def setEncodingMeta(key: String, value: Array[Byte]) = {
    encodingMetaMap += key->value
    db.put(DistributedKeyConverter.indexEncoderKeyToBytes(key), value)

    udpClientManager.sendRefreshMsg()
  }

  def deleteEncodingMeta(key: String): Unit ={
    encodingMetaMap.remove(key)
    db.delete(DistributedKeyConverter.indexEncoderKeyToBytes(key))

    udpClientManager.sendRefreshMsg()
  }


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

    udpClientManager.sendRefreshMsg()
  }

  def delete(labelName: String, propertyName: String): Unit = {
    val id = nodeStore.getLabelId(labelName)
    val pid = nodeStore.getPropertyKeyId(propertyName)
    if (id.isDefined && pid.isDefined){
      val key = keyWithIndexFunc(id.get, pid.get)
      db.delete(key)

      indexMetaMap(labelName).remove(propertyName)
      if (indexMetaMap(labelName).isEmpty) indexMetaMap -= labelName

      udpClientManager.sendRefreshMsg()
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

    val encodingPrefix = encodingKeyPrefix()
    val eIter = db.scanPrefix(encodingPrefix, 10000, false)
    while (eIter.hasNext){
      val data = eIter.next()
      val key = data.getKey.toByteArray
      val value = data.getValue.toByteArray
      val encoderKey = new String(key, 1, key.length - 1)
      encodingMetaMap += encoderKey -> value
    }
  }

  def refreshIndexMeta(): Unit ={
    var tmpIndexMetaMap: mutable.Map[String, mutable.Set[String]] = mutable.Map[String, mutable.Set[String]]()
    var tmpEncodingMetaMap: mutable.Map[String, Array[Byte]] = mutable.Map[String, Array[Byte]]()

    val prefix = keyPrefixFunc()
    val iter = db.scanPrefix(prefix, 10000, true)
    while (iter.hasNext){
      val key = iter.next().getKey.toByteArray
      val label = nodeStore.getLabelName(ByteUtils.getInt(key, 1)).get
      val property = nodeStore.getPropertyKeyName(ByteUtils.getInt(key, 5)).get
      if (tmpIndexMetaMap.contains(label)) tmpIndexMetaMap(label).add(property)
      else tmpIndexMetaMap += label -> mutable.Set(property)
    }

    val encodingPrefix = encodingKeyPrefix()
    val eIter = db.scanPrefix(encodingPrefix, 10000, false)
    while (eIter.hasNext){
      val data = eIter.next()
      val key = data.getKey.toByteArray
      val value = data.getValue.toByteArray
      val encoderKey = new String(key, 1, key.length - 1)
      tmpEncodingMetaMap += encoderKey -> value
    }

    indexMetaMap = tmpIndexMetaMap
    encodingMetaMap = tmpEncodingMetaMap
  }
}
