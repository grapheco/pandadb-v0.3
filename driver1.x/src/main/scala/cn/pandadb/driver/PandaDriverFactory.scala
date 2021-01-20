package cn.pandadb.driver

import java.security.KeyFactory
import java.security.spec.X509EncodedKeySpec

import cn.pandadb.{UnknownErrorException, UsernameOrPasswordErrorException, VerifyConnectionMode}
import cn.pandadb.hipporpc.PandaRpcClient
import cn.pandadb.hipporpc.utils.RegexUtils
import javax.crypto.Cipher
import org.apache.commons.codec.binary.Base64
import org.neo4j.driver.internal.security.InternalAuthToken
import org.neo4j.driver.v1.Value

import scala.io.StdIn

class PandaDriverFactory(uriAuthority: String, authtoken: java.util.Map[String, Value], config: PandaDriverConfig) {

  def newInstance(): PandaDriver ={
    val res = RegexUtils.getIpAndPort(uriAuthority)
    val _address = res._1
    val _port = res._2
    val rpcClient = new PandaRpcClient(_address, _port, config.RPC_CLIENT_NAME, config.RPC_SERVER_NAME)

    val publicKey = rpcClient.getPublicKey()
    val username = authtoken.get(InternalAuthToken.PRINCIPAL_KEY).asString()
    val password = authtoken.get(InternalAuthToken.CREDENTIALS_KEY).asString()
    verifyConnectivity(rpcClient, username, password, publicKey)

    new PandaDriver(rpcClient, uriAuthority)
  }

  def verifyConnectivity(client: PandaRpcClient, username: String, password: String, publicKey: String): Unit ={
    val res = client.verifyConnectionRequest(rsaEncrypt(username, publicKey), rsaEncrypt(password, publicKey))
    if (res == VerifyConnectionMode.ERROR) {
      client.shutdown()
      throw new UsernameOrPasswordErrorException
    }else if (res == VerifyConnectionMode.EDIT)
    {
      println("First login, please reset your account and password")
      val username = StdIn.readLine("username: ")
      val password = StdIn.readLine("password: ")
      val res = client.resetAccountRequest(rsaEncrypt(username, publicKey), rsaEncrypt(password, publicKey))
      if (res == VerifyConnectionMode.RESET_FAILED){
        throw new UnknownErrorException("reset account")
      }
    }
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
