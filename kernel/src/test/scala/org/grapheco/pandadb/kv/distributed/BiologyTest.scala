package org.grapheco.pandadb.kv.distributed

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.junit.{After, Before, Test}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.raw.RawKVClient

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2022-03-28 18:36
 */
class BiologyTest {
  var api: DistributedGraphFacade = _

  var tikv: RawKVClient = _

  val kvHosts = "10.0.82.144:2379,10.0.82.145:2379,10.0.82.146:2379"
  val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
  val udpClient = Array(new UDPClient("127.0.0.1", 6000))
  @Before
  def init(): Unit = {
    val conf = TiConfiguration.createRawDefault(kvHosts)
    val session = TiSession.create(conf)
    tikv = session.createRawClient()
    api = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
  }
  @After
  def close(): Unit = {
    api.close()
  }
  @Test
  def cypherTest(): Unit ={
    api.cypher(
      """
        |MATCH (t:taxonomy {tax_id:'9606'})-[r:taxonomy2bioproject]->(b:bioproject) RETURN t.scientific_name as scientific_name, b.bioproject_id as bioproject_id, b.title as title, b.cen as cen SKIP 0 LIMIT 10
        |""".stripMargin).show()
  }
}
