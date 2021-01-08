package cn.pandadb.dbms

import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.security.{KeyFactory, KeyPairGenerator, SecureRandom}

import javax.crypto.Cipher
import org.apache.commons.codec.binary.Base64

object RsaSecurity {
  private var keyMap: Map[Int, String] = Map()

  def init(): Unit ={
    val keyPairGenerator = KeyPairGenerator.getInstance("RSA")
    keyPairGenerator.initialize(1024, new SecureRandom())

    val keyPair = keyPairGenerator.generateKeyPair()

    val privateKey = keyPair.getPrivate
    val privateKeyStr = new String(Base64.encodeBase64(privateKey.getEncoded))

    val publicKey = keyPair.getPublic
    val publicKeyStr = new String(Base64.encodeBase64(publicKey.getEncoded))

    keyMap += 0 -> privateKeyStr
    keyMap += 1 -> publicKeyStr
  }

  def getPublicKeyStr(): String ={
    keyMap(1)
  }
  def getPrivateKeyStr(): String ={
    keyMap(0)
  }

  def rsaEncrypt(content: String, publicKey: String): String ={
    val encoded = Base64.decodeBase64(publicKey)
    val rsaPublicKey = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(encoded))

    val cipher = Cipher.getInstance("RSA")
    cipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey)

    Base64.encodeBase64String(cipher.doFinal(content.getBytes("UTF-8")))
  }

  def rsaDecrypt(content : String, privateKey : String): String ={
    val bytes = Base64.decodeBase64(content.getBytes("UTF-8"))
    val decoded = Base64.decodeBase64(privateKey)

    val rsaPrivateKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(decoded))
    val cipher = Cipher.getInstance("RSA")
    cipher.init(Cipher.DECRYPT_MODE, rsaPrivateKey)
    new String(cipher.doFinal(bytes))
  }
}
