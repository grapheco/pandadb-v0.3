package cn.pandadb.kernel

import cn.pandadb.kernel.distribute.DistributedKeyConverter
import cn.pandadb.kernel.kv.ByteUtils
import cn.pandadb.tools.importer.GlobalArgs

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:47 2020/12/3
 * @Modified By:
 */
object PDBMetaData {

  private val _propIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _typeIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _labelIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)

  def persist(globalArgs: GlobalArgs): Unit = {
    val db = globalArgs.metaDB

    _labelIdManager.all.foreach{
      kv=>
        val key = DistributedKeyConverter.nodeLabelKeyToBytes(kv._1)
        db.put(key, ByteUtils.stringToBytes(kv._2))
    }
    _typeIdManager.all.foreach{
      kv=>
        val key = DistributedKeyConverter.relationTypeKeyToBytes(kv._1)
        db.put(key, ByteUtils.stringToBytes(kv._2))
    }
    _propIdManager.all.foreach{
      kv=>
        val key = DistributedKeyConverter.propertyNameKeyToBytes(kv._1)
        db.put(key, ByteUtils.stringToBytes(kv._2))
    }
  }

  def isPropExists(prop: String): Boolean = _propIdManager.isNameExists(prop)

  def isLabelExists(label: String): Boolean = _labelIdManager.isNameExists(label)

  def isTypeExists(edgeType: String): Boolean = _typeIdManager.isNameExists(edgeType)

  def getPropId(prop: String): Int = {
    _propIdManager.getId(prop)
  }

  def getPropName(propId: Int): String = {
    _propIdManager.getName(propId)
  }

  def getLabelId(label: String): Int = {
    _labelIdManager.getId(label)
  }

  def getLabelName(labelId: Int): String = {
    _labelIdManager.getName(labelId)
  }

  def getTypeId(edgeType: String): Int = {
    _typeIdManager.getId(edgeType)
  }

  def getTypeName(typeId: Int): String = {
    _typeIdManager.getName(typeId)
  }
}
