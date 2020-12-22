package cn.pandadb.kernel

import java.io.{File, FileInputStream, FileOutputStream}

import cn.pandadb.kernel.util.serializer.ChillSerializer
import org.rocksdb.RocksDB

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:47 2020/12/3
 * @Modified By:
 */
object PDBMetaData {
  private var _propIdMap: Map[String, Int] = Map[String, Int]()
  private var _rPropIdMap: Map[Int, String] = Map[Int, String]()
  private var _labelIdMap: Map[String, Int] = Map[String, Int]()
  private var _rLabelIdMap: Map[Int, String] = Map[Int, String]()
  private var _typeIdMap: Map[String, Int] = Map[String, Int]()
  private var _rTypeIdMap: Map[Int, String] = Map[Int, String]()

  private var _propCounter: Int = 0
  private var _labelCounter: Int = 0
  private var _typeCounter: Int = 0

  def persist(rocksDB: RocksDB): Unit = {
    val metaData: Map[String, Any] = Map("_propIdMap" -> _propIdMap, "_rPropIdMap" -> _rPropIdMap,
      "_labelIdMap" -> _labelIdMap, "_rLabelIdMap" -> _rLabelIdMap,
      "_typeIdMap" -> _typeIdMap, "_rTypeIdMap" -> _rTypeIdMap,
      "_propCounter" -> _propCounter, "_labelCounter" -> _labelCounter, "_typeCounter" -> _typeCounter)
    val bytes: Array[Byte] = ChillSerializer.serialize(metaData)
    rocksDB.put("meta".getBytes(), bytes)
  }

  def init(rocksDB: RocksDB): Unit = {
    val bytes: Array[Byte] = rocksDB.get("meta".getBytes())
    val metaData: Map[String, Any] = ChillSerializer.deserialize(bytes, classOf[Map[String, Any]])
    _propIdMap = metaData("_propIdMap").asInstanceOf[Map[String, Int]]
    _rPropIdMap = metaData("_rPropIdMap").asInstanceOf[Map[Int, String]]
    _labelIdMap = metaData("_labelIdMap").asInstanceOf[Map[String, Int]]
    _rLabelIdMap = metaData("_rLabelIdMap").asInstanceOf[Map[Int, String]]
    _typeIdMap = metaData.get("_typeIdMap").get.asInstanceOf[Map[String, Int]]
    _rTypeIdMap = metaData.get("_rTypeIdMap").get.asInstanceOf[Map[Int, String]]
    _propCounter = metaData.get("_propCounter").get.asInstanceOf[Int]
    _labelCounter = metaData.get("_labelCounter").get.asInstanceOf[Int]
    _typeCounter = metaData.get("_typeCounter").get.asInstanceOf[Int]
  }

  def isPropExists(prop: String): Boolean = _propIdMap.contains(prop)

  def isLabelExists(label: String): Boolean = _labelIdMap.contains(label)

  def isTypeExists(edgeType: String): Boolean = _typeIdMap.contains(edgeType)

  def addProp(prop: String): Int = {
    if (!isPropExists(prop)) {
      _propIdMap += (prop -> _propCounter)
      _rPropIdMap += (_propCounter -> prop)
      _propCounter += 1
      _propCounter - 1
    } else _propIdMap(prop)
  }

  def addLabel(label: String): Int = {
    if (!isLabelExists(label)) {
      _labelIdMap += (label -> _labelCounter)
      _rLabelIdMap += (_labelCounter -> label)
      _labelCounter += 1
      _labelCounter - 1
    } else _labelIdMap(label)
  }

  def addType(edgeType: String): Int = {
    if (!isTypeExists(edgeType)) {
      _typeIdMap += (edgeType -> _typeCounter)
      _rTypeIdMap += (_typeCounter -> edgeType)
      _typeCounter += 1
      _typeCounter - 1
    } else _typeIdMap(edgeType)
  }

  def getPropId(prop: String): Int = {
    if (isPropExists(prop)) _propIdMap(prop)
    else addProp(prop)
  }

  def getPropName(propId: Int): String = {
    _rPropIdMap(propId)
  }

  def getLabelId(label: String): Int = {
    if (isLabelExists(label)) _labelIdMap(label)
    else addLabel(label)
  }

  def getLabelName(labelId: Int): String = {
    _rLabelIdMap(labelId)
  }

  def getTypeId(edgeType: String): Int = {
    if (isTypeExists(edgeType)) _typeIdMap(edgeType)
    else addType(edgeType)
  }

  def getTypeName(typeId: Int): String = {
    _rTypeIdMap(typeId)
  }
}
