package cn.pandadb.test.cypher.cs

import java.io.File

import cn.pandadb.server.PandaServerBootstrapper
import org.apache.commons.io.FileUtils

/**
 * @program: pandadb-v0.3
 * @description: a server for driver test
 * @author: LiamGao
 * @create: 2021-04-26
 */
object ServerForTest {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File("./panda-test/testdata/server"))
    val server = new PandaServerBootstrapper()
    server.start(new File("./panda-test/testdata/pandadb.conf"))
  }
}
