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
    Assert.assertEquals("10.0.82.143:2379,10.0.82.144:2379,10.0.82.145:2379", config.getKVHosts())
    Assert.assertEquals("10.0.82.144:9200,10.0.82.145:9200,10.0.82.146:9200", config.getIndexHosts())
  }
}
