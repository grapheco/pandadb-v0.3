package cn.pandadb.itest

import java.io.File

import cn.pandadb.server.DistributedPandaServerBootstrapper

/**
 * @program: pandadb-v0.3
 * @description:
 * @author: LiamGao
 * @create: 2021-12-31 11:00
 */
object Server3 {
  def main(args: Array[String]): Unit = {
    val confFile = new File("./testdata/cluster/p3.conf")
    val serverBootstrapper = new DistributedPandaServerBootstrapper()
    serverBootstrapper.start(confFile)
  }
}
