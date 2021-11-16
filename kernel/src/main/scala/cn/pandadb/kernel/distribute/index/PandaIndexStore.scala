package cn.pandadb.kernel.distribute.index

import cn.pandadb.kernel.distribute.meta.MetaNameMap
import cn.pandadb.kernel.util.PandaDBException.PandaDBException
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder

import scala.collection.JavaConverters._
import scala.collection.mutable
/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 11:17
 */

trait DistributedIndexStore {
  type StatusResponse = Int
  def serviceIsAvailable(): Boolean
  def indexIsExist(indexNames: String*): Boolean
  def cleanIndexes(indexNames: String*): Unit

  def createIndex(indexName: String, settings: Map[String, Any]): Boolean
  def deleteIndex(indexName: String): Boolean

  def addMetaDoc(indexName: String, key: String, value: Int): StatusResponse
  def deleteMetaDoc(indexName: String, id: Int): StatusResponse
  def searchDoc(indexName: String, key: String): Option[Map[String, Int]]
  def searchDoc(indexName: String, id: Int): Option[Map[String, Int]]
  def loadAllMeta(indexName: String): (Map[String, Int], Map[Int, String])
}

class PandaDistributedIndexStore(client: RestHighLevelClient) extends DistributedIndexStore{
  override def serviceIsAvailable(): Boolean = {
    try {
      client.ping(RequestOptions.DEFAULT)
    }catch {
      case e: Exception =>throw new PandaDBException("elasticSearch cluster can't access...")
    }
  }

  override def indexIsExist(indexNames: String*): Boolean = {
    client.indices().exists(new GetIndexRequest(indexNames:_*), RequestOptions.DEFAULT)
  }

  override def cleanIndexes(indexNames: String*): Unit = {
    indexNames.foreach(name => {
      deleteIndex(name)
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

  override def addMetaDoc(indexName: String, name: String, id: Int): Int = {
    val data = Map(MetaNameMap.metaName-> name, MetaNameMap.metaId -> id).asInstanceOf[Map[String, Object]].asJava
    val jsonString = JSON.toJSONString(data, SerializerFeature.QuoteFieldNames)

    val request = new IndexRequest(indexName).id(id.toString).source(jsonString, XContentType.JSON)
    val res = client.index(request, RequestOptions.DEFAULT)
    res.status().getStatus
  }

  override def deleteMetaDoc(indexName: String, id: Int): Int = {
    val request = new DeleteRequest(indexName, id.toString)
    val response = client.delete(request, RequestOptions.DEFAULT)
    response.status().getStatus
  }

  override def searchDoc(indexName: String, key: String): Option[Map[String, Int]] = {
    val request = new SearchRequest().indices(indexName)
    val builder = new SearchSourceBuilder()
    builder.query(QueryBuilders.termQuery(s"${MetaNameMap.metaName}.keyword", key))
    request.source(builder)
    val res = client.search(request, RequestOptions.DEFAULT)
    res.getHits.getHits.headOption.map(f => f.getSourceAsMap.asScala.toMap.asInstanceOf[Map[String, Int]])
  }

  override def searchDoc(indexName: String, id: Int): Option[Map[String, Int]] = {
    val request = new SearchRequest().indices(indexName)
    val builder = new SearchSourceBuilder()
    builder.query(QueryBuilders.termQuery(s"${MetaNameMap.metaId}.keyword", id))
    request.source(builder)
    val res = client.search(request, RequestOptions.DEFAULT)
    res.getHits.getHits.headOption.map(f => f.getSourceAsMap.asScala.toMap.asInstanceOf[Map[String, Int]])
  }

  override def loadAllMeta(indexName: String): (Map[String, Int], Map[Int, String]) = {
    val iter = new MetaDataIterator(indexName, client)
    var mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
    var mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()
    while (iter.hasNext){
      val data = iter.next()
      data.foreach(li => {
        mapString2Int += li._1 -> li._2
        mapInt2String += li._2 -> li._1
      })
    }
    (mapString2Int.toMap, mapInt2String.toMap)
  }
}

class MetaDataIterator(indexName: String, client:RestHighLevelClient) extends Iterator[Seq[(String, Int)]] {
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
    data.map(kv => (kv(MetaNameMap.metaName).toString, kv(MetaNameMap.metaId).asInstanceOf[Int])).toSeq
  }
}

