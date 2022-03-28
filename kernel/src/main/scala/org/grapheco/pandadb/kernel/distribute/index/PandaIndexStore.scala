package org.grapheco.pandadb.kernel.distribute.index

import java.util
import org.grapheco.pandadb.kernel.distribute.{DistributedGraphService, DistributedKVAPI}
import org.grapheco.pandadb.kernel.distribute.index.utils.{IndexTool, IndexValueConverter}
import org.grapheco.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping, NodeLabelNameStore, PropertyNameStore}
import org.grapheco.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import org.grapheco.pandadb.kernel.store.{IndexNode, PandaNode}
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, MultiGetRequest}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{MultiSearchRequest, SearchRequest, SearchRequestBuilder}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.core.TimeValue
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.ingest.Processor
import org.elasticsearch.script.Script
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.grapheco.lynx.{Index, LynxNodeLabel, LynxPropertyKey, LynxValue, NodeFilter}
import org.grapheco.pandadb.kernel.distribute.DistributedKVAPI
import org.grapheco.pandadb.kernel.distribute.index.utils.IndexValueConverter
import org.grapheco.pandadb.kernel.distribute.meta.{DistributedStatistics, NameMapping}
import org.grapheco.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import org.grapheco.pandadb.kernel.udp.UDPClientManager

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 11:17
 */
class PandaDistributedIndexStore(client: RestHighLevelClient,
                                 _db: DistributedKVAPI, graphService: DistributedGraphService,
                                 dupClientManager: UDPClientManager) extends IndexStoreService {
  type NodeID = Long
  type NodeLabel = String
  type NodeLabels = List[String]
  type NodePropertyName = String
  type NodePropertyValue = Any

  val nodeIndexMetaStore = new NodeIndexMetaStore(_db, graphService, dupClientManager)
  val indexTool = new IndexTool(client)

  if (!indexTool.indexIsExist(NameMapping.indexName)) indexTool.createIndex(NameMapping.indexName)

  // for udp
  override def refreshIndexMeta(): Unit ={
    nodeIndexMetaStore.refreshIndexMeta()
  }

  override def getIndexMeta: Map[String, Seq[String]] = {
    nodeIndexMetaStore.indexMetaMap.map(p => (p._1, p._2.toSeq)).toMap
  }

  override def getEncodingMeta: Map[String, Array[Byte]] = nodeIndexMetaStore.getEncodingMetaMap

  override def removeEncodingMeta(name: String): Unit = nodeIndexMetaStore.deleteEncodingMeta(name)

  override def setEncodingMeta(name: String, value: Array[Byte]): Unit = nodeIndexMetaStore.setEncodingMeta(name, value)

  def getDB() = _db

  override def getIndexTool() = indexTool

  override def cleanIndexes(indexName: String*): Unit ={
    indexTool.cleanIndexes(indexName:_*)
  }

  override def getNodeByDocId(docId: String): IndexNode ={
    val request = new GetRequest().index(NameMapping.indexName).id(docId)
    val res = client.get(request, RequestOptions.DEFAULT)
    IndexValueConverter.transferDoc2IndexNode(res.getId, res.getSourceAsMap.asScala.toMap)
  }

  override def deleteNodeByNodeId(nodeId: String): Unit = {
    val node = getNodeByDocId(nodeId)
    indexTool.deleteDoc(nodeId)
    node.props.keySet.foreach(pn => graphService.getStatistics.decreaseIndexPropertyCount(graphService.getPropertyId(pn).get, 1))
  }

  override def searchNodes(labels: Seq[String], filter: Map[String, Any]): Iterator[Seq[Long]] = {
    val indexedLabels = labels.intersect(nodeIndexMetaStore.indexMetaMap.keySet.toSeq)
    if (indexedLabels.nonEmpty){
      val data = IndexValueConverter.transferNode2Doc(nodeIndexMetaStore.indexMetaMap, indexedLabels, filter)

      val queryBuilder = new BoolQueryBuilder()
      indexedLabels.foreach(labelName => queryBuilder.must(QueryBuilders.termQuery(s"${NameMapping.indexNodeLabelColumnName}.keyword", labelName)))
      data.foreach(propNameAndValue => {
        propNameAndValue._2 match {
          case stringProp: String => queryBuilder.must(QueryBuilders.termQuery(s"${propNameAndValue._1}.keyword", propNameAndValue._2))
          case _ =>  queryBuilder.must(QueryBuilders.termQuery(s"${propNameAndValue._1}", propNameAndValue._2))
        }
      })

      new SearchNodeIterator(queryBuilder)
    }
    else Iterator.empty
  }

  class SearchNodeIterator(queryBuilder: QueryBuilder) extends Iterator[Seq[Long]]{
    var dataBatch: Seq[String] = _
    var page = 0
    val pageSize = 1000
    val request = new SearchRequest().indices(NameMapping.indexName)

    override def hasNext: Boolean = {
      val builder = new SearchSourceBuilder().query(queryBuilder)
        .from(page * pageSize)
        .size(pageSize)
      request.source(builder)
      dataBatch = client.search(request, RequestOptions.DEFAULT).getHits.asScala.map(f => f.getId).toSeq
      page += 1
      dataBatch.nonEmpty
    }

    override def next(): Seq[Long] = dataBatch.map(f => f.toLong)
  }

  override def isIndexCreated(targetLabel: String, propNames: Seq[String]): Boolean ={
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    if (indexMetaMap.contains(targetLabel)){
      propNames.intersect(indexMetaMap(targetLabel).toSeq).size == propNames.size
    }
    else false
  }

  override def isNodeHasIndex(filter: NodeFilter): Boolean ={
    val labels = filter.labels.map(f => f.value)
    val propNames = filter.properties.keySet.toSeq.map(f => f.value)
    val indexedLabels = labels.intersect(nodeIndexMetaStore.indexMetaMap.keySet.toSeq)
    if (indexedLabels.nonEmpty){
      val indexedProps = indexedLabels.flatMap(label => propNames.intersect(nodeIndexMetaStore.indexMetaMap(label).toSeq))
      if (indexedProps.nonEmpty) true
      else false
    }
    else false
  }

  override def setIndexOnSingleNode(nodeId: Long, labels: Seq[String], nodeProps: Map[String, Any]): Unit ={
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    val indexedLabel = labels.intersect(indexMetaMap.keySet.toSeq)
    if (indexedLabel.nonEmpty){
      val indexedData = indexedLabel.map(iLabel => iLabel -> indexMetaMap(iLabel).intersect(nodeProps.keySet))
      indexedData.foreach(labelAndProps => {
        if (labelAndProps._2.nonEmpty){
          val data = IndexValueConverter.transferNode2Doc(indexMetaMap, Seq(labelAndProps._1), labelAndProps._2.map(name => name -> nodeProps(name)).toMap)
          if (indexTool.isDocExist(nodeId.toString))
            client.update(updateNodeRequest(NameMapping.indexName, nodeId, labels, data.toMap), RequestOptions.DEFAULT)
          else
            client.index(addNewNodeRequest(NameMapping.indexName, nodeId, labels, data.toMap), RequestOptions.DEFAULT)
          labelAndProps._2.foreach(propName => graphService.getStatistics.increaseIndexPropertyCount(graphService.getPropertyId(propName).get, 1))
        }
      })
    }
  }

  override def dropIndexOnSingleNodeProp(nodeId: Long, labels: Seq[String], dropPropName: String): Unit ={
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    val indexedLabel = labels.intersect(indexMetaMap.keySet.toSeq)
    if (indexedLabel.nonEmpty){
      indexedLabel.foreach(iLabel => {
        if (indexMetaMap(iLabel).contains(dropPropName)){
          deleteIndexField(NameMapping.indexName, nodeId, iLabel, dropPropName)
          graphService.getStatistics.decreaseIndexPropertyCount(graphService.getPropertyId(dropPropName).get, 1)
        }
      })
    }
  }

  override def dropIndexOnSingleNodeLabel(nodeId: Long, label: String): Unit = {
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    if (indexMetaMap.contains(label)){
      indexMetaMap(label).foreach(propName => {
        deleteIndexField(NameMapping.indexName, nodeId, label, propName)
        graphService.getStatistics.decreaseIndexPropertyCount(graphService.getPropertyId(propName).get, 1)
      })
    }
  }

  override def isLabelHasEncoder(label: String): Boolean = {
    val encoderMetaMap = nodeIndexMetaStore.encodingMetaMap
    encoderMetaMap.exists(p => p._1.split("\\.")(0) == label)
  }

  override def batchAddIndexOnNodes(targetLabel: String, targetPropNames: Seq[String], nodes: Iterator[PandaNode]): Unit ={
    var nodeCount = 0
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    val labelHasEncoder = isLabelHasEncoder(targetLabel)

    val noIndexProps = targetPropNames.diff(indexMetaMap.getOrElse(targetLabel, Set.empty).toSeq)
    if (noIndexProps.nonEmpty){
      val processor = indexTool.getBulkProcessor(2000, 3)
      indexTool.setIndexToBatchMode(NameMapping.indexName)
      while (nodes.hasNext){
        val node = nodes.next()
        val indexedLabels = node.labels.intersect(indexMetaMap.keySet.toSeq)
        val nodeHasIndex = indexedLabels.nonEmpty
        val data = noIndexProps.map(propName => (s"$targetLabel.$propName", node.property(LynxPropertyKey(propName)).get.value)).toMap
        if (indexMetaMap.contains(targetLabel) || nodeHasIndex || labelHasEncoder) processor.add(updateNodeRequest(NameMapping.indexName, node.id.value, node.labels.map(_.value), data))
        else processor.add(addNewNodeRequest(NameMapping.indexName, node.id.value, node.labels.map(_.value), data))

        nodeCount += 1
      }
      processor.flush()
      processor.close()
      indexTool.setIndexToNormalMode(NameMapping.indexName)

      noIndexProps.foreach(name => nodeIndexMetaStore.addToDB(targetLabel, name))
      noIndexProps.foreach(name => graphService.getStatistics.increaseIndexPropertyCount(graphService.getPropertyId(name).get, nodeCount))
    }
  }

  override def batchDropEncoder(label: String, encoderName: String, nodes: Iterator[PandaNode]): Unit ={
    val labelIndexMeta = nodeIndexMetaStore.indexMetaMap.keySet.toSeq

    val processor = indexTool.getBulkProcessor(2000, 3)
    indexTool.setIndexToBatchMode(NameMapping.indexName)
    while (nodes.hasNext){
      val node = nodes.next()
      val hasIndexLabel = node.labels.intersect(labelIndexMeta).nonEmpty
      if (hasIndexLabel) processor.add(deleteIndexField(NameMapping.indexName, node.id.value, label, encoderName))
      else processor.add(deleteDocRequest(NameMapping.indexName, node.id.value.toString))
    }
    processor.flush()
    processor.close()
    indexTool.setIndexToNormalMode(NameMapping.indexName)
  }

  override def batchDropIndexLabelWithProperty(indexLabel: String, targetPropName: String, nodes: Iterator[PandaNode]): Unit ={
    val labelHasEncoder = isLabelHasEncoder(indexLabel)

    val processor = indexTool.getBulkProcessor(2000, 3)
    indexTool.setIndexToBatchMode(NameMapping.indexName)
    var nodeCount = 0
    while (nodes.hasNext){
      val node = nodes.next()
      if (nodeIndexMetaStore.indexMetaMap(indexLabel).size == 1 && !labelHasEncoder) processor.add(deleteDocRequest(NameMapping.indexName, node.id.value.toString))
      else processor.add(deleteIndexField(NameMapping.indexName, node.id.value, indexLabel, targetPropName))

      nodeCount += 1
    }
    processor.flush()
    processor.close()
    indexTool.setIndexToNormalMode(NameMapping.indexName)

    nodeIndexMetaStore.delete(indexLabel, targetPropName)
    graphService.getStatistics.decreaseIndexPropertyCount(graphService.getPropertyId(targetPropName).get, nodeCount)
  }

  def addNewNodeRequest(indexName: String, nodeId: Long, labels: Seq[String], transferProps: Map[NodePropertyName, NodePropertyValue]): IndexRequest = {
    val data = (Map(NameMapping.indexNodeLabelColumnName -> labels.asJava) ++ transferProps).asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)
    new IndexRequest(indexName).id(nodeId.toString).source(jsonString, XContentType.JSON)
  }

  def updateNodeRequest(indexName: String, nodeId: Long, labels: Seq[String], transferProps: Map[String, Any]): UpdateRequest = {
    val request = new UpdateRequest()
    val _data = (Map(NameMapping.indexNodeLabelColumnName -> labels.asJava) ++ transferProps).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(_data, SerializerFeature.QuoteFieldNames)
    request.index(indexName).id(nodeId.toString)
    request.doc(jsonString, XContentType.JSON)
    request
  }

  def addExtraProperty(indexName: String, nodeId: Long, transferProps: Map[String, Any]): UpdateRequest ={
    val request = new UpdateRequest()
    val _data = transferProps.asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(_data, SerializerFeature.QuoteFieldNames)
    request.index(indexName).id(nodeId.toString)
    request.doc(jsonString, XContentType.JSON)
    request
  }

  def deleteIndexField(indexName: String, nodeId: Long, label: String, propertyName: String): UpdateRequest = {
    val script = s"ctx._source.remove('$label.$propertyName')"
    new UpdateRequest().index(indexName).id(nodeId.toString).script(new Script(script))
  }

  def deleteDocRequest(indexName: String, docId: String): DeleteRequest = {
    new DeleteRequest().index(indexName).id(docId)
  }
}