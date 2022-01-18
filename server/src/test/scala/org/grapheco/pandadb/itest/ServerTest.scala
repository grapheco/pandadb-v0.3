package org.grapheco.pandadb.itest

import java.io.File

import org.grapheco.pandadb.server.DistributedPandaServerBootstrapper
import org.junit.Test


class ServerTest {
  @Test
  def testConfStart(): Unit ={
    val confFile = new File("./testdata/pandadb.conf")
    val serverBootstrapper = new DistributedPandaServerBootstrapper()
    serverBootstrapper.start(confFile)
  }
}
