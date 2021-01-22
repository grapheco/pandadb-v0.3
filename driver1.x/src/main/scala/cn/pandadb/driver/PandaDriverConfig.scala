package cn.pandadb.driver

object PandaDriverConfig {

  def defaultConfiguration(): PandaDriverConfig = {
    new PandaDriverConfig("pandadb-server", "pandadb-client")
  }
}

class PandaDriverConfig(rpcServerName: String, rpcClientName: String){
  val RPC_SERVER_NAME: String = rpcServerName
  val RPC_CLIENT_NAME: String = rpcClientName
}
