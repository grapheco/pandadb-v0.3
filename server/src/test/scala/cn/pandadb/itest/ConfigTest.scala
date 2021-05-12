package cn.pandadb.itest

import java.io.File

import cn.pandadb.server.common.configuration.Config
import org.junit.{Assert, Test}

/**
 * @program: pandadb-v0.3
 * @description: test config
 * @author: LiamGao
 * @create: 2021-05-12 12:08
 */
class ConfigTest {
  @Test
  def testConfig(): Unit ={
    val configFile = new File("./testdata/pandadb.conf")
    val config = new Config().withFile(Option(configFile)).withSettings(Map("db.default.home.path"->"/test/pandadb-v0.3"))
    Assert.assertEquals("0.0.0.0", config.getListenHost())
    Assert.assertEquals(9989, config.getRpcPort())
    Assert.assertEquals("pandadb-server", config.getRpcServerName())
    Assert.assertEquals("default", config.getRocksdbConfigFilePath())
    Assert.assertEquals("/test/pandadb-v0.3/data/pandadb.db/nodeMeta", config.getNodeMetaDBPath())
    Assert.assertEquals("/test/pandadb-v0.3/data/pandadb.db/index", config.getIndexDBPath())
    Assert.assertEquals("/test/data/pandadb.db/nodeLabel", config.getNodeLabelDBPath())

  }
}
