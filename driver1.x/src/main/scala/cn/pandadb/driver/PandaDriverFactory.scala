package cn.pandadb.driver

import java.security.KeyFactory
import java.security.spec.X509EncodedKeySpec

import cn.pandadb.driver.rpc.PandaRpcClient
import cn.pandadb.{UnknownErrorException, UsernameOrPasswordErrorException, VerifyConnectionMode}
import javax.crypto.Cipher
import org.apache.commons.codec.binary.Base64
import org.neo4j.driver.internal.security.InternalAuthToken
import org.neo4j.driver.v1.Value

import scala.io.StdIn

class PandaDriverFactory(uriAuthority: String, authtoken: java.util.Map[String, Value], config: PandaDriverConfig) {

  def newInstance(): PandaDriver ={
//    val publicKey = rpcClient.getPublicKey()
//    val username = authtoken.get(InternalAuthToken.PRINCIPAL_KEY).asString()
//    val password = authtoken.get(InternalAuthToken.CREDENTIALS_KEY).asString()
//    verifyConnectivity(rpcClient, username, password, publicKey)

    new PandaDriver(uriAuthority, config)
  }

//  def verifyConnectivity(client: PandaRpcClient, username: String, password: String, publicKey: String): Unit ={
//    val res = client.verifyConnectionRequest(rsaEncrypt(username, publicKey), rsaEncrypt(password, publicKey))
//    if (res == VerifyConnectionMode.ERROR) {
//      client.shutdown()
//      throw new UsernameOrPasswordErrorException
//    }else if (res == VerifyConnectionMode.EDIT)
//    {
//      println("First login, please reset your password")
//      println("username: pandadb")
//      val password = StdIn.readLine("password: ")
//      val res = client.resetAccountRequest(rsaEncrypt("pandadb", publicKey), rsaEncrypt(password, publicKey))
//      if (res == VerifyConnectionMode.RESET_FAILED){
//        throw new UnknownErrorException("reset account")
//      }
//    }
//  }
//
//  def getPublicKey(client: PandaRpcClient): String ={
//    client.getPublicKey()
//  }
//
//  def rsaEncrypt(content: String, publicKey: String): String ={
//    val encoded = Base64.decodeBase64(publicKey)
//    val rsaPublicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(encoded))
//
//    val cipher = Cipher.getInstance("RSA")
//    cipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey)
//
//    Base64.encodeBase64String(cipher.doFinal(content.getBytes("UTF-8")))
//  }
}
