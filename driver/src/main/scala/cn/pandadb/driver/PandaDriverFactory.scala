package cn.pandadb.driver

import java.security.KeyFactory
import java.security.spec.X509EncodedKeySpec

import cn.pandadb.driver.utils.RegexUtils
import javax.crypto.Cipher
import org.apache.commons.codec.binary.Base64

class PandaDriverFactory(uri: String, authtoken: PandaAuthToken, config: PandaDriverConfig) {

  def newInstance(): PandaDriver ={
    val res = RegexUtils.getIpAndPort(uri)
    val address = res._1
    val port = res._2

    val rpcClient = new PandaRpcClient(address, port, config.RPC_CLIENT_NAME, config.RPC_SERVER_NAME)

    val publicKey = rpcClient.getPublicKey()

    verifyConnectivity(rpcClient, rsaEncrypt(authtoken.username, publicKey), rsaEncrypt(authtoken.password, publicKey))

    new PandaDriver(rpcClient)
  }

  def verifyConnectivity(client: PandaRpcClient, username: String, password: String): Unit ={
    val res = client.verifyConnectionRequest(username, password)
    if (res == "no") throw new UsernameOrPasswordErrorException
  }

  def getPublicKey(client: PandaRpcClient): String ={
    client.getPublicKey()
  }

  def rsaEncrypt(content: String, publicKey: String): String ={
    val encoded = Base64.decodeBase64(publicKey)
    val rsaPublicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(encoded))

    val cipher = Cipher.getInstance("RSA")
    cipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey)

    Base64.encodeBase64String(cipher.doFinal(content.getBytes("UTF-8")))
  }
}
