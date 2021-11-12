package cn.pandadb.itest

import java.io.{File}

import cn.pandadb.server.{PandaServerBootstrapper}
import org.junit.{Test}


class ServerTest {

  @Test
  def testConfStart(): Unit ={
//    FileUtils.deleteDirectory(new File("./testdb"))

    val confFile = new File("./testdata/pandadb.conf")
    val serverBootstrapper = new PandaServerBootstrapper()
    serverBootstrapper.start(confFile, Map("db.default.home.path" -> "./testdb"))
  }
}
