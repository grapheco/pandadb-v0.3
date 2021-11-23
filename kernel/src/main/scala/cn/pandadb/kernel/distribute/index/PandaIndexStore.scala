package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.index.utils.IndexConverter
import cn.pandadb.kernel.distribute.meta.NameMapping
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.core.TimeValue
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}
import org.elasticsearch.script.Script
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._
import scala.collection.{mutable}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 11:17
 */
class PandaDistributedIndexStore(client: RestHighLevelClient) extends DistributedIndexStore {
  private val nodeIndexMetaStore = new NodeIndexMetaStore(this)
  private val relationIndexMetaStore = new RelationIndexMetaStore(this)

  val indexes = Array(NameMapping.nodeIndex, NameMapping.relationIndex)
  indexes.foreach(name => {
    if (!indexIsExist(name)) createIndex(name)
  })

  // es index
  override def serviceIsAvailable(): Boolean = {
    try {
      client.ping(RequestOptions.DEFAULT)
    } catch {
      case e: Exception => throw new PandaDBException("elasticSearch cluster can't access...")
    }
  }

  override def indexIsExist(indexNames: String*): Boolean = {
    client.indices().exists(new GetIndexRequest(indexNames: _*), RequestOptions.DEFAULT)
  }

  override def cleanIndexes(indexNames: String*): Unit = {
    indexNames.foreach(name => {
      if (indexIsExist(name)) deleteIndex(name)
      createIndex(name, Map("refresh_interval" -> "1s"))
    })
  }

  override def createIndex(indexName: String, settings: Map[String, Any] = Map.empty): Boolean = {
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

  override def deleteIndex(indexName: String): Boolean = {
    client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT).isAcknowledged
  }

  // common
  override def searchDoc(filter: Seq[(String, Any)], indexName: String): Iterator[Seq[String]] = {
    val boolBuilder = new BoolQueryBuilder()
    filter.foreach(kv => {
      val value = IndexConverter.transferType2Java(kv._2)
      IndexConverter.value2TermQuery(boolBuilder, kv._1, value)
    })
    new IndexSearchHitIds(indexName, client, boolBuilder)
  }

  override def deleteDoc(indexName: String, docId: String): Unit = {
    val request = new DeleteRequest().index(indexName).id(docId)
    client.delete(request, RequestOptions.DEFAULT)
  }

  // name store
  override def addNameMetaDoc(indexName: String, name: String, id: Int): Int = {
    val data = Map(NameMapping.metaName -> name, NameMapping.metaId -> id).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)

    val request = new IndexRequest(indexName).id(id.toString).source(jsonString, XContentType.JSON)
    val res = client.index(request, RequestOptions.DEFAULT)
    res.status().getStatus
  }

  override def searchNameMetaDoc(indexName: String, key: String): Option[Map[String, Int]] = {
    val request = new SearchRequest().indices(indexName)
    val builder = new SearchSourceBuilder()
    builder.query(QueryBuilders.termQuery(s"${NameMapping.metaName}.keyword", key))
    request.source(builder)
    val res = client.search(request, RequestOptions.DEFAULT)
    res.getHits.getHits.headOption.map(f => f.getSourceAsMap.asScala.toMap.asInstanceOf[Map[String, Int]])
  }

  override def searchNameMetaDoc(indexName: String, id: Int): Option[Map[String, Int]] = {
    val request = new SearchRequest().indices(indexName)
    val builder = new SearchSourceBuilder()
    builder.query(QueryBuilders.idsQuery().addIds(id.toString))
    request.source(builder)
    val res = client.search(request, RequestOptions.DEFAULT)
    res.getHits.getHits.headOption.map(f => f.getSourceAsMap.asScala.toMap.asInstanceOf[Map[String, Int]])
  }

  override def loadAllMeta(indexName: String): Iterator[Seq[Map[String, AnyRef]]] = {
    new MetaDataIterator(indexName, client)
  }

  // db index meta
  override def addIndexField(_id: Long, label: String, propertyName: String, propValue: Any, indexName: String): Unit = {
    _addIndexMetaDoc(indexName, label, propertyName)
    val data = Map(NameMapping.indexMetaLabelName -> label, propertyName -> propValue).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)
    val request = new IndexRequest(indexName).id(_id.toString).source(jsonString, XContentType.JSON)
    client.index(request, RequestOptions.DEFAULT)
  }

  override def updateIndexField(_id: Long, label: String, propertyName: String, propValue: Any, indexName: String): Unit = {
    _addIndexMetaDoc(indexName, label, propertyName)
    val request = new UpdateRequest()
    val _data = Map(propertyName -> propValue).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(_data, SerializerFeature.QuoteFieldNames)
    request.index(indexName).id(_id.toString)
    request.doc(jsonString, XContentType.JSON)
    client.update(request, RequestOptions.DEFAULT)
  }

  override def deleteIndexField(label: String, propertyName: String, indexName: String): Unit = {
    _deleteIndexMetaDoc(indexName, label, propertyName)

    setIndexToBatchMode(indexName)
    val processor = getBulkProcessor(1000, 5)
    val iter = new IndexAllDataIds(indexName, client)
    while (iter.hasNext) {
      val batchDocIds = iter.next()
      batchDocIds.foreach(_id => {
        val script = s"ctx._source.remove('$propertyName')"
        val request = new UpdateRequest().index(indexName).id(_id.toString).script(new Script(script))
        processor.add(request)
      })
    }
    processor.flush()
    processor.close()
    setIndexToNormalMode(indexName)
  }

  private def _addIndexMetaDoc(indexName: String, label: String, propertyName: String): Unit ={
    indexName match{
      case NameMapping.nodeIndex => nodeIndexMetaStore.existOrAdd(label, propertyName)
      case NameMapping.relationIndex => relationIndexMetaStore.existOrAdd(label, propertyName)
    }
  }
  private def _deleteIndexMetaDoc(indexName: String, label: String, propertyName: String): Unit ={
    indexName match{
      case NameMapping.nodeIndex => nodeIndexMetaStore.delete(label, propertyName)
      case NameMapping.relationIndex => relationIndexMetaStore.delete(label, propertyName)
    }
  }


  // index meta store
  override def addIndexMetaDoc(indexName: String, label: String, property: String): Unit = {

    val data = Map(
      NameMapping.indexMetaLabelName -> label,
      NameMapping.indexMetaPropertyName -> property
    ).asInstanceOf[Map[String, Object]].asJava

    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)
    val request = new IndexRequest(indexName).source(jsonString, XContentType.JSON)
    client.index(request, RequestOptions.DEFAULT)
  }

  override def deleteIndexMetaDoc(indexName: String, label: String, propertyName: String): Unit = {
    val metaIter = searchDoc(Seq(NameMapping.indexMetaLabelName -> label, NameMapping.indexMetaPropertyName -> propertyName), indexName)
    metaIter.next().foreach(id => deleteDoc(indexName, id))
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
      .put("refresh_interval", "30s")
      .put("translog.durability", "request")
      .build()
    request.settings(settings)
    val response = client.indices().putSettings(request, RequestOptions.DEFAULT)
    response.isAcknowledged
  }

  def getBulkProcessor(batchSize: Int, concurrentThreadNums: Int): BulkProcessor = {
    val listener: BulkProcessor.Listener = new BulkProcessor.Listener() {
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      }

      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
      }

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
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

class MetaDataIterator(indexName: String, client: RestHighLevelClient) extends Iterator[Seq[Map[String, AnyRef]]] {
  var flag = true
  var page = 0
  val batch = 1000
  val request = new SearchRequest().indices(indexName)
  val builder = new SearchSourceBuilder()

  override def hasNext: Boolean = flag

  override def next(): Seq[Map[String, AnyRef]] = {
    builder.query(QueryBuilders.matchAllQuery())
      .from(page * batch)
      .size(batch)

    request.source(builder)
    val response = client.search(request, RequestOptions.DEFAULT)
    val data = response.getHits.iterator().asScala.map(f => f.getSourceAsMap.asScala.toMap)
    page += 1
    if (data.isEmpty) flag = false
    data.toSeq
    //    data.map(kv => (kv(NameMapping.metaName).toString, kv(NameMapping.metaId).asInstanceOf[Int])).toSeq
  }
}

class IndexAllDataIds(indexName: String, client: RestHighLevelClient) extends Iterator[Seq[String]] {
  var flag = true
  var page = 0
  val batch = 1000
  val request = new SearchRequest().indices(indexName)
  val builder = new SearchSourceBuilder()

  override def hasNext: Boolean = flag

  override def next(): Seq[String] = {
    builder.query(QueryBuilders.matchAllQuery())
      .from(page * batch)
      .size(batch)

    request.source(builder)
    val response = client.search(request, RequestOptions.DEFAULT)
    val data = response.getHits.iterator().asScala.map(hits => hits.getId)
    page += 1
    if (data.isEmpty) flag = false
    data.toSeq
  }
}

class IndexSearchHitIds(indexName: String, client: RestHighLevelClient, boolQueryBuilder: BoolQueryBuilder) extends Iterator[Seq[String]] {
  var flag = true
  var page = 0
  val batch = 1000
  val request = new SearchRequest().indices(indexName)
  val builder = new SearchSourceBuilder()

  override def hasNext: Boolean = flag

  override def next(): Seq[String] = {
    builder.query(boolQueryBuilder)
      .from(page * batch)
      .size(batch)
    request.source(builder)
    val response = client.search(request, RequestOptions.DEFAULT)
    val data = response.getHits.iterator().asScala.map(hits => hits.getId).toSeq
    page += 1
    if (data.isEmpty) flag = false
    data
  }
}

