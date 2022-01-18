package org.grapheco.pandadb.tools.importer

import org.grapheco.pandadb.kernel.distribute.DistributedGraphFacade
import org.grapheco.pandadb.kernel.udp.{UDPClient, UDPClientManager}
import org.junit.Test

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 21:29 2021/1/15
  * @Modified By:
  */

class ImporterTest {
  @Test
  def importStatsData(): Unit = {
    val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
    val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
    val importCmd = s"./importer-panda.sh --nodes=src/test/input/testdata.csv --delimeter=, --array-delimeter=| --kv-hosts=$kvHosts --index-hosts=$indexHosts".split(" ")
    PandaImporter.main(importCmd)

    println("nodes")
    PandaImporter.importerStatics.getNodeCountByLabel.foreach(kv => println(kv._1, kv._2))
    println(PandaImporter.importerStatics.getGlobalNodeCount)
    println(PandaImporter.importerStatics.getGlobalNodePropCount)
    println("rels")
    PandaImporter.importerStatics.getRelCountByType.foreach(kv => println(kv._1, kv._2))
    println(PandaImporter.importerStatics.getGlobalRelCount)
  }
  @Test
  def statistics(): Unit ={
    val kvHosts = "10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379"
    val indexHosts = "10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200"
    val udpClient = Array(new UDPClient("127.0.0.1", 6000))

    val importCmd = s"./importer-panda.sh --nodes=src/test/input/biology.node.trick.csv  --delimeter=☔ --array-delimeter=| --kv-hosts=$kvHosts".split(" ")
//    val importCmd = s"./importer-panda.sh --nodes=src/test/input/biology.node.trick.csv --relationships=src/test/input/biology.rel.trick.csv --delimeter=☔ --array-delimeter=| --kv-hosts=$kvHosts".split(" ")
    PandaImporter.main(importCmd)

    val db = new DistributedGraphFacade(kvHosts, indexHosts, new UDPClientManager(udpClient))
    println(s"all nodes: ${db.statistics._allNodesCount}")
    db.statistics._nodeCountByLabel.foreach(kv => println(s"node label id: ${kv._1}, count: ${kv._2}"))
    db.statistics._propertyCountByIndex.foreach(kv => println(s"node index prop id: ${kv._1}, count: ${kv._2}"))

    println(s"all relations: ${db.statistics._allRelationCount}")
    db.statistics._relationCountByType.foreach(kv => println(s"relation type id: ${kv._1}, count: ${kv._2}"))
    db.cypher("match (n) return n limit 10").show()
    db.close()
  }
}
