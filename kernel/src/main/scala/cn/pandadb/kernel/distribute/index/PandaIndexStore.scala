package cn.pandadb.kernel.distribute.index

import java.util

import cn.pandadb.kernel.distribute.DistributedKVAPI
import cn.pandadb.kernel.distribute.index.utils.{IndexConverter, SearchConfig}
import cn.pandadb.kernel.distribute.meta.{NameMapping, NodeLabelNameStore, PropertyNameStore}
import cn.pandadb.kernel.distribute.node.DistributedNodeStoreSPI
import cn.pandadb.kernel.store.PandaNode
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
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
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.ingest.Processor
import org.elasticsearch.script.Script
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.grapheco.lynx.{LynxValue, NodeFilter}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 11:17
 */
class PandaDistributedIndexStore(client: RestHighLevelClient, _db: DistributedKVAPI, nls: DistributedNodeStoreSPI) {
  type NodeID = Long
  type NodeLabel = String
  type NodeLabels = List[String]
  type NodePropertyName = String
  type NodePropertyValue = Any

  val nodeIndexMetaStore = new NodeIndexMetaStore(_db, nls)

  if (!indexIsExist(NameMapping.indexName)) createIndex(NameMapping.indexName)

  // es index
  def serviceIsAvailable(): Boolean = {
    try {
      client.ping(RequestOptions.DEFAULT)
    } catch {
      case e: Exception => throw new PandaDBException("elasticSearch cluster can't access...")
    }
  }

  def indexIsExist(indexNames: String*): Boolean = {
    client.indices().exists(new GetIndexRequest(indexNames: _*), RequestOptions.DEFAULT)
  }

  def cleanIndexes(indexNames: String*): Unit = {
    indexNames.foreach(name => {
      if (indexIsExist(name)) deleteIndex(name)
      createIndex(name, Map("refresh_interval" -> "1s"))
    })
  }

  def createIndex(indexName: String, settings: Map[String, Any] = Map.empty): Boolean = {
    val request = new CreateIndexRequest(indexName)
    val settingsBuilder = Settings.builder()
    // default settings
    settingsBuilder.put("index.number_of_shards", 5)
      .put("index.number_of_replicas", 0)
      .put("max_result_window", 50000000)
      .put("translog.flush_threshold_size", "1g")
      .put("refresh_interval", "30s")
      .put("translog.durability", "request")
    //new settings
    settings.foreach {
      kv => {
        kv._2 match {
          case n: String => settingsBuilder.put(kv._1, n)
          case n: Int => settingsBuilder.put(kv._1, n)
          case n: Long => settingsBuilder.put(kv._1, n)
          case n: Double => settingsBuilder.put(kv._1, n)
          case n: Float => settingsBuilder.put(kv._1, n)
          case n: Boolean => settingsBuilder.put(kv._1, n)
        }
      }
    }

    request.settings(settingsBuilder)
    client.indices().create(request, RequestOptions.DEFAULT).isAcknowledged
  }

  def deleteIndex(indexName: String): Boolean = {
    client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT).isAcknowledged
  }

  def isDocExist(docId: String): Boolean = {
    val request = new GetRequest().index(NameMapping.indexName).id(docId)
    client.exists(request, RequestOptions.DEFAULT)
  }
  def deleteDoc(docId: String) = {
    val request = new DeleteRequest().index(NameMapping.indexName).id(docId)
    client.delete(request, RequestOptions.DEFAULT)
  }

  def searchNodes(labels: Seq[String], filter: Map[String, Any]): Iterator[Seq[PandaNode]] = {
    val indexedLabels = labels.intersect(nodeIndexMetaStore.indexMetaMap.keySet.toSeq)
    if (indexedLabels.nonEmpty){
      val data = IndexConverter.transferNode2Doc(nodeIndexMetaStore.indexMetaMap, indexedLabels, filter)

      val queryBuilder = new BoolQueryBuilder()
      indexedLabels.foreach(labelName => queryBuilder.must(QueryBuilders.termQuery(s"${NameMapping.indexNodeLabelColumnName}", labelName)))
      data.foreach(propNameAndValue => queryBuilder.must(QueryBuilders.termQuery(s"${propNameAndValue._1}", propNameAndValue._2)))

      new SearchNodeIterator(queryBuilder)
    }
    else Iterator.empty
  }

  class SearchNodeIterator(queryBuilder: BoolQueryBuilder) extends Iterator[Seq[PandaNode]]{
    var dataBatch: Seq[PandaNode] = _
    var page = 0
    val pageSize = 1000
    val request = new SearchRequest().indices(NameMapping.indexName)

    override def hasNext: Boolean = {
      val builder = new SearchSourceBuilder().query(queryBuilder)
        .from(page * pageSize)
        .size(pageSize)
      request.source(builder)
      val iter = client.search(request, RequestOptions.DEFAULT).getHits.iterator().asScala.map(f => (f.getId, f.getSourceAsMap.asScala.toMap))
      dataBatch = iter.map(doc => {
        val nodeId = doc._1.toLong
        val labels = doc._2(NameMapping.indexNodeLabelColumnName).asInstanceOf[util.ArrayList[String]].asScala.toSeq
        val props = doc._2 - NameMapping.indexNodeLabelColumnName
        val cleanProps = props.map(pv => pv._1.split("\\.")(1)->LynxValue(pv._2))
        PandaNode(nodeId, labels, cleanProps.toSeq:_*)
      }).toSeq

      page += 1

      dataBatch.nonEmpty
    }

    override def next(): Seq[PandaNode] = dataBatch
  }


  def isIndexCreated(targetLabel: String, propNames: Seq[String]): Boolean ={
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    if (indexMetaMap.contains(targetLabel)){
      propNames.intersect(indexMetaMap(targetLabel).toSeq).size == propNames.size
    }
    else false
  }

  def isNodeHasIndex(filter: NodeFilter): Boolean ={
    val labels = filter.labels
    val propNames = filter.properties.keySet.toSeq
    val indexedLabels = labels.intersect(nodeIndexMetaStore.indexMetaMap.keySet.toSeq)
    if (indexedLabels.nonEmpty){
      val indexedProps = indexedLabels.flatMap(label => propNames.intersect(nodeIndexMetaStore.indexMetaMap(label).toSeq))
      if (indexedProps.nonEmpty) true
      else false
    }
    else false
  }

  def addIndexOnSingleNode(nodeId: Long, labels: Seq[String], nodeProps: Map[String, Any]): Unit ={
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    val indexedLabel = labels.intersect(indexMetaMap.keySet.toSeq)
    if (indexedLabel.nonEmpty){
      val data = IndexConverter.transferNode2Doc(indexMetaMap, indexedLabel, nodeProps)
      client.index(addNewNodeRequest(NameMapping.indexName, nodeId, labels, data.toMap), RequestOptions.DEFAULT)
    }
  }


  def batchAddIndexOnNodes(targetLabel: String, targetPropNames: Seq[String], nodes: Iterator[PandaNode]): Unit ={
    val indexMetaMap = nodeIndexMetaStore.indexMetaMap
    val noIndexProps = targetPropNames.diff(indexMetaMap(targetLabel).toSeq)
    if (noIndexProps.nonEmpty){
      val processor = getBulkProcessor(2000, 3)
      setIndexToBatchMode(NameMapping.indexName)
      while (nodes.hasNext){
        val node = nodes.next()
        val nodeHasIndex = node.labels.intersect(indexMetaMap.keySet.toSeq).nonEmpty
        val data = noIndexProps.map(propName => (s"$targetLabel.$propName", node.property(propName).get.value)).toMap
        if (indexMetaMap.contains(targetLabel) || nodeHasIndex) processor.add(updatePropertyRequest(NameMapping.indexName, node.longId, data))
        else processor.add(addNewNodeRequest(NameMapping.indexName, node.longId, node.labels, data))
      }
      processor.flush()
      processor.close()
      setIndexToNormalMode(NameMapping.indexName)

      noIndexProps.foreach(name => nodeIndexMetaStore.addToDB(targetLabel, name))
    }
  }

  def batchDeleteIndexLabelWithProperty(indexLabel: String, targetPropName: String, nodes: Iterator[PandaNode]): Unit ={
    val processor = getBulkProcessor(2000, 3)
    setIndexToBatchMode(NameMapping.indexName)
    while (nodes.hasNext){
      val node = nodes.next()
      if (nodeIndexMetaStore.indexMetaMap(indexLabel).size == 1) processor.add(deleteDocRequest(NameMapping.indexName, node.longId.toString))
      else processor.add(deleteIndexField(NameMapping.indexName, node.longId, indexLabel, targetPropName))
    }
    processor.flush()
    processor.close()
    setIndexToNormalMode(NameMapping.indexName)

    nodeIndexMetaStore.delete(indexLabel, targetPropName)
  }

  private def addNewNodeRequest(indexName: String, nodeId: Long, labels: Seq[String], transferProps: Map[NodePropertyName, NodePropertyValue]): IndexRequest = {
    val data = (Map(NameMapping.indexNodeLabelColumnName -> labels.asJava) ++ transferProps).asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)
    new IndexRequest(indexName).id(nodeId.toString).source(jsonString, XContentType.JSON)
  }

  private def updatePropertyRequest(indexName: String, nodeId: Long, transferProps: Map[String, Any]): UpdateRequest = {
    val request = new UpdateRequest()
    val _data = transferProps.asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(_data, SerializerFeature.QuoteFieldNames)
    request.index(indexName).id(nodeId.toString)
    request.doc(jsonString, XContentType.JSON)
    request
  }

  private def deleteIndexField(indexName: String, nodeId: Long, label: String, propertyName: String): UpdateRequest = {
    val script = s"ctx._source.remove('$label.$propertyName')"
    new UpdateRequest().index(indexName).id(nodeId.toString).script(new Script(script))
  }
  private def deleteDocRequest(indexName: String, docId: String): DeleteRequest = {
    new DeleteRequest().index(indexName).id(docId)
  }

  def setIndexToBatchMode(indexName: String): Boolean = {
    val request = new UpdateSettingsRequest(indexName)
    val settings = Settings.builder()
      .put("refresh_interval", "300s")
      .put("translog.durability", "async")
      .build()
    request.settings(settings)
    val response = client.indices().putSettings(request, RequestOptions.DEFAULT)
    response.isAcknowledged
  }

  def setIndexToNormalMode(indexName: String): Boolean = {
    val request = new UpdateSettingsRequest(indexName)
    val settings = Settings.builder()
      .put("refresh_interval", "1s") // 1s for test
      .put("translog.durability", "request")
      .build()
    request.settings(settings)
    val response = client.indices().putSettings(request, RequestOptions.DEFAULT)
    response.isAcknowledged
  }

  def getBulkProcessor(batchSize: Int, concurrentThreadNums: Int): BulkProcessor = {
    val listener: BulkProcessor.Listener = new BulkProcessor.Listener() {
      def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      }

      def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
      }

      def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
        println(failure)
      }
    }

    val builder: BulkProcessor.Builder = BulkProcessor.builder((request: BulkRequest, bulkListener: ActionListener[BulkResponse])
    => client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener, "bulk-processor")

    builder.setBulkActions(batchSize)
    builder.setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
    builder.setConcurrentRequests(concurrentThreadNums) // default is 1
    builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1), 3))

    builder.build()
  }
}