package org.grapheco.pandadb.kernel.distribute.index.utils

import org.grapheco.pandadb.kernel.distribute.meta.NameMapping
import org.grapheco.pandadb.kernel.util.PandaDBException.PandaDBException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue}
import org.elasticsearch.core.TimeValue
import org.grapheco.pandadb.kernel.distribute.meta.NameMapping

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-01-07 15:47
 */
class IndexTool(client: RestHighLevelClient) {
  def isDocExist(docId: String): Boolean = {
    val request = new GetRequest().index(NameMapping.indexName).id(docId)
    client.exists(request, RequestOptions.DEFAULT)
  }
  def deleteDoc(docId: String) = {
    val request = new DeleteRequest().index(NameMapping.indexName).id(docId)
    client.delete(request, RequestOptions.DEFAULT)
  }
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
      .put("index.number_of_replicas", 3)
      .put("max_result_window", 50000000)
      .put("translog.flush_threshold_size", "1g")
      .put("refresh_interval", "1s")
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
