package cn.pandadb.driver

import cn.pandadb.driver.utils.RegexUtils

class PandaDriverFactory(uri: String, authtoken: PandaAuthToken, config: PandaDriverConfig) {

  def newInstance(): PandaDriver ={
    val res = RegexUtils.getIpAndPort(uri)
    val address = res._1
    val port = res._2

    val rpcClient = new PandaRpcClient(address, port, config.RPC_CLIENT_NAME, config.RPC_SERVER_NAME)

    verifyConnectivity(rpcClient, authtoken)

    new PandaDriver(rpcClient)
  }

  def verifyConnectivity(client: PandaRpcClient, authToken: PandaAuthToken): Unit ={
    val res = client.verifyConnectionRequest(authtoken.username, authToken.password)
    if (res == "no") throw new UsernameOrPasswordErrorException
  }
}
