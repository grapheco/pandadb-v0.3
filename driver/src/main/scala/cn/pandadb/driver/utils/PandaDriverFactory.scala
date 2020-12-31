package cn.pandadb.driver.utils

import cn.pandadb.driver.{PandaDriver, PandaRpcClient, UsernameOrPasswordErrorException}

class PandaDriverFactory(uri: String, authtoken: PandaAuthToken, config: PandaDriverConfig) {

  def newInstance(): PandaDriver ={
    val address = uri.split(":")(0)
    val port = uri.split(":")(1).toInt

    val rpcClient = new PandaRpcClient(address, port, config.RPC_CLIENT_NAME, config.RPC_SERVER_NAME)

    verifyConnectivity(rpcClient, authtoken)

    new PandaDriver(rpcClient)
  }

  def verifyConnectivity(client: PandaRpcClient, authToken: PandaAuthToken): Unit ={
    val res = client.verifyConnectionRequest(authtoken.username, authToken.password)
    if (res == "no") throw new UsernameOrPasswordErrorException
  }
}
