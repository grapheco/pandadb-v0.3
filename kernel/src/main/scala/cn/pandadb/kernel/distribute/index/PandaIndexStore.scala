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
import org.grapheco.lynx.{LynxBoolean, LynxDate, LynxDateTime, LynxDouble, LynxDuration, LynxInteger, LynxList, LynxLocalDateTime, LynxLocalTime, LynxNumber, LynxString, LynxTime}

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 11:17
 */

trait DistributedIndexStore {
  type StatusResponse = Int
  type NodeId = Long
  type KeyName = String
  type DataValue = Any

  def serviceIsAvailable(): Boolean

  // es index
  def indexIsExist(indexNames: String*): Boolean

  def cleanIndexes(indexNames: String*): Unit

  def createIndex(indexName: String, extraSettings: Map[String, Any]): Boolean

  def deleteIndex(indexName: String): Boolean

  // doc
  def addDoc(indexName: String, key: String, value: Int): StatusResponse

  def deleteDoc(indexName: String, id: Int): StatusResponse

  def searchDoc(indexName: String, key: String): Option[Map[String, Int]]

  def searchDoc(indexName: String, id: Int): Option[Map[String, Int]]

  // meta to Map
  def loadAllMeta(indexName: String): (Map[String, Int], Map[Int, String])

  // db index
  def addIndexField(fieldName: KeyName, data: Iterator[(NodeId, DataValue)], indexName: String = NameMapping.nodeIndex)

  // another index added
  def updateIndexField(fieldName: KeyName, data: Iterator[(NodeId, DataValue)], indexName: String = NameMapping.nodeIndex)

  def deleteIndexField(fieldName: KeyName, indexName: String = NameMapping.nodeIndex)

  // new data added
  def addSingleIndexField(dataId: String, fieldName: KeyName, value: DataValue, indexName: String = NameMapping.nodeIndex)

  def searchData(filter: Seq[(String, Any)], indexName: String = NameMapping.nodeIndex): IndexSearchHitIds
}

class PandaDistributedIndexStore(client: RestHighLevelClient) extends DistributedIndexStore {
  val nodeIndexMetaStore = new NodeIndexNameStore(this)
  val relationIndexMetaStore = new RelationIndexNameStore(this)

  override def addIndexField(fieldName: KeyName, data: Iterator[(NodeId, DataValue)], indexName: String): Unit = {
    addIndexMeta(indexName, fieldName)

    setIndexToBatchMode(indexName)

    val processor = getBulkProcessor(1000, 5)
    data.foreach(node => {
      val data = Map(fieldName -> node._2).asInstanceOf[Map[String, Object]].asJava
      val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)
      val request = new IndexRequest(indexName).id(node._1.toString).source(jsonString, XContentType.JSON)
      processor.add(request)
    })
    processor.flush()
    processor.close()

    setIndexToNormalMode(indexName)
  }

  override def updateIndexField(fieldName: KeyName, data: Iterator[(NodeId, DataValue)], indexName: String): Unit = {
    addIndexMeta(indexName, fieldName)

    setIndexToBatchMode(indexName)
    val processor = getBulkProcessor(1000, 5)

    data.foreach(node => {
      val request = new UpdateRequest()
      val _data = Map(fieldName -> node._2).asInstanceOf[Map[String, Object]].asJava
      val jsonString = JSON.toJSONString(_data, SerializerFeature.QuoteFieldNames)
      request.index(indexName).id(node._1.toString)
      request.doc(jsonString, XContentType.JSON)
      processor.add(request)
    })
    processor.flush()
    processor.close()
    setIndexToNormalMode(indexName)
  }

  override def deleteIndexField(fieldName: KeyName, indexName: String): Unit = {
    deleteIndexMeta(indexName, fieldName)

    setIndexToBatchMode(indexName)
    val processor = getBulkProcessor(1000, 5)

    val iter = new IndexAllDataIds(indexName, client)
    while (iter.hasNext) {
      val batchDocIds = iter.next()
      batchDocIds.foreach(_id => {
        val script = s"ctx._source.remove('$fieldName')"
        val request = new UpdateRequest().index(indexName).id(_id.toString).script(new Script(script))
        processor.add(request)
      })
    }
    processor.flush()
    processor.close()
    setIndexToNormalMode(indexName)
  }

  // for new data added to index
  override def addSingleIndexField(dataId: String, fieldName: KeyName, value: DataValue, indexName: String): Unit = {
    val data = Map(fieldName -> value).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)
    val request = new IndexRequest(indexName).id(dataId).source(jsonString, XContentType.JSON)
    client.index(request, RequestOptions.DEFAULT)
  }

  override def searchData(filter: Seq[(String, Any)], indexName: String): IndexSearchHitIds = {
    val boolBuilder = new BoolQueryBuilder()
    filter.foreach(kv => {
      val value = IndexConverter.transferType2Java(kv._2)
      IndexConverter.value2TermQuery(boolBuilder, kv._1, value)
    })
    new IndexSearchHitIds(indexName, client, boolBuilder)
  }

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
      createIndex(name)
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

  override def addDoc(indexName: String, name: String, id: Int): Int = {
    val data = Map(NameMapping.metaName -> name, NameMapping.metaId -> id).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)

    val request = new IndexRequest(indexName).id(id.toString).source(jsonString, XContentType.JSON)
    val res = client.index(request, RequestOptions.DEFAULT)
    res.status().getStatus
  }

  override def deleteDoc(indexName: String, id: Int): Int = {
    val request = new DeleteRequest(indexName, id.toString)
    val response = client.delete(request, RequestOptions.DEFAULT)
    response.status().getStatus
  }

  override def searchDoc(indexName: String, key: String): Option[Map[String, Int]] = {
    val request = new SearchRequest().indices(indexName)
    val builder = new SearchSourceBuilder()
    builder.query(QueryBuilders.termQuery(s"${NameMapping.metaName}.keyword", key))
    request.source(builder)
    val res = client.search(request, RequestOptions.DEFAULT)
    res.getHits.getHits.headOption.map(f => f.getSourceAsMap.asScala.toMap.asInstanceOf[Map[String, Int]])
  }

  override def searchDoc(indexName: String, id: Int): Option[Map[String, Int]] = {
    val request = new SearchRequest().indices(indexName)
    val builder = new SearchSourceBuilder()
    builder.query(QueryBuilders.idsQuery().addIds(id.toString))
    request.source(builder)
    val res = client.search(request, RequestOptions.DEFAULT)
    res.getHits.getHits.headOption.map(f => f.getSourceAsMap.asScala.toMap.asInstanceOf[Map[String, Int]])
  }

  override def loadAllMeta(indexName: String): (Map[String, Int], Map[Int, String]) = {
    val iter = new MetaDataIterator(indexName, client)
    var mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
    var mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()
    while (iter.hasNext) {
      val data = iter.next()
      data.foreach(li => {
        mapString2Int += li._1 -> li._2
        mapInt2String += li._2 -> li._1
      })
    }
    (mapString2Int.toMap, mapInt2String.toMap)
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
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {}

      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {}

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

  def addIndexMeta(indexName: String, metaName: String): Unit = {
    indexName match {
      case NameMapping.nodeIndex => nodeIndexMetaStore.getOrAddId(metaName)
      case NameMapping.relationIndex => relationIndexMetaStore.getOrAddId(metaName)
    }
  }

  def deleteIndexMeta(indexName: String, metaName: String): Unit = {
    indexName match {
      case NameMapping.nodeIndex => nodeIndexMetaStore.delete(metaName)
      case NameMapping.relationIndex => relationIndexMetaStore.delete(metaName)
    }
  }
}

class MetaDataIterator(indexName: String, client: RestHighLevelClient) extends Iterator[Seq[(String, Int)]] {
  var flag = true
  var page = 0
  val batch = 1000
  val request = new SearchRequest().indices(indexName)
  val builder = new SearchSourceBuilder()

  override def hasNext: Boolean = flag

  override def next(): Seq[(String, Int)] = {
    builder.query(QueryBuilders.matchAllQuery())
      .from(page * batch)
      .size(batch)

    request.source(builder)
    val response = client.search(request, RequestOptions.DEFAULT)
    val data = response.getHits.iterator().asScala.map(f => f.getSourceAsMap.asScala.toMap)
    page += 1
    if (data.isEmpty) flag = false
    data.map(kv => (kv(NameMapping.metaName).toString, kv(NameMapping.metaId).asInstanceOf[Int])).toSeq
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

