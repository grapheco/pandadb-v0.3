package cn.pandadb.kv.distributed.index

import cn.pandadb.kernel.distribute.index.{PandaDistributedIndexStore}
import cn.pandadb.kernel.distribute.meta.NameMapping
import org.apache.http.HttpHost
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.script.Script
import org.junit.{Before, Test}

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-11-15 14:36
 */
class IndexStoreTest {
  val hosts = Array(new HttpHost("10.0.82.144", 9200, "http"),
    new HttpHost("10.0.82.145", 9200, "http"),
    new HttpHost("10.0.82.146", 9200, "http"))

  var client: RestHighLevelClient = _

  @Before
  def init(): Unit = {
    client = new RestHighLevelClient(RestClient.builder(hosts: _*))
  }


  @Test
  def d(): Unit ={
    val script = s"ctx._source.remove('label')"
    val request = new UpdateRequest().index(NameMapping.indexName).id(1.toString).script(new Script(script))
    client.update(request, RequestOptions.DEFAULT)
  }
}
