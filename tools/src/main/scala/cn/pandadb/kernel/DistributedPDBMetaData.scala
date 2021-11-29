package cn.pandadb.kernel

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import cn.pandadb.kernel.distribute.index.{DistributedIndexStore, PandaDistributedIndexStore}
import cn.pandadb.kernel.distribute.meta.NameMapping
import cn.pandadb.kernel.kv.{ByteUtils, KeyConverter, RocksDBStorage}
import cn.pandadb.kernel.util.DBNameMap
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:47 2020/12/3
 * @Modified By:
 */
object DistributedPDBMetaData {
  val hosts = Array(new HttpHost("10.0.82.144", 9200, "http"),
    new HttpHost("10.0.82.145", 9200, "http"),
    new HttpHost("10.0.82.146", 9200, "http"))

  val nodeMeta = NameMapping.nodeLabelMetaName
  val nodePropertyMeta = NameMapping.nodePropertyMetaName
  val nodeIndex = NameMapping.nodeIndex
  val nodeIndexMeta = NameMapping.nodeIndexMeta

  val relationMeta = NameMapping.relationTypeMetaName
  val relationPropertyMeta = NameMapping.relationPropertyMetaName
  val relationIndex = NameMapping.relationIndex
  val relationIndexMeta = NameMapping.relationIndexMeta

  val indexNames = Array(nodeMeta, nodePropertyMeta, nodeIndex, nodeIndexMeta, relationMeta,
    relationPropertyMeta, relationIndex, relationIndexMeta)

  val client = new RestHighLevelClient(RestClient.builder(hosts: _*))
  val indexStore = new PandaDistributedIndexStore(client)
  indexStore.cleanIndexes(indexNames:_*)

  private val _nodeIdAllocator: AtomicLong = new AtomicLong(0)
  private val _relationIdAllocator: AtomicLong = new AtomicLong(0)

  def availableNodeId: Long = _nodeIdAllocator.getAndIncrement()
  def availableRelId: Long = _relationIdAllocator.getAndIncrement()

  private val _propIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _typeIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _labelIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)

  def persist(): Unit = {

    _labelIdManager.all.foreach{
      kv=>
        indexStore.addNameMetaDoc(nodeMeta, kv._2, kv._1)
    }
    _typeIdManager.all.foreach{
      kv=>
        indexStore.addNameMetaDoc(relationMeta, kv._2, kv._1)
    }
    _propIdManager.all.foreach{
      kv=>
        indexStore.addNameMetaDoc(nodePropertyMeta, kv._2, kv._1)
        indexStore.addNameMetaDoc(relationPropertyMeta, kv._2, kv._1)
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
