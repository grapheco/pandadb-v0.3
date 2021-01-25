package cn.pandadb.server.common.configuration

import java.io.{File, FileInputStream}
import java.util.Properties

import cn.pandadb.server.common.Logging

import scala.collection.mutable
import scala.collection.JavaConverters._


object SettingKeys {
  val version = "db.version"
  val rpcListenHost = "dbms.server.rpc.listen.host"
  val rpcListenPort = "dbms.server.rpc.listen.port"
  val rpcServerName = "dbms.server.rpc.service.name"
  val dataRpcEndpointName = "dbms.server.rpc.data.endpoint"

  val localDataPath = "db.data.path"
  val localDBName = "db.name"

}

class Config extends Logging {
  private val settingsMap = new mutable.HashMap[String, String]()

  def withFile(configFile: Option[File]): Config = {
    if (configFile.isDefined) {
      val props = new Properties()
      props.load(new FileInputStream(configFile.get))
      settingsMap ++= props.asScala
    }
    this
  }

  def withSettings(settings: Map[String, String]): Config = {
    settingsMap ++= settings
    this
  }

  def validate(): Unit = {}

  def getListenHost(): String = {
    getValueAsString(SettingKeys.rpcListenHost, "127.0.0.1")
  }
  def getRpcPort(): Int = {
    getValueAsInt(SettingKeys.rpcListenPort, 52000)
  }
  def getNodeAddress(): String = {getListenHost + ":" + getRpcPort.toString}

  def getRpcServerName(): String = {
    getValueAsString(SettingKeys.rpcServerName, "pandadb-server")
  }

  def getDataServiceEndpointName(): String = {
    getValueAsString(SettingKeys.dataRpcEndpointName, "data-endpoint")
  }

  def getLocalDataStorePath(): String = {
    getValueAsString(SettingKeys.localDataPath, "not setting")
  }

  def getLocalDBName(): String ={
    getValueAsString(SettingKeys.localDBName, defaultValue = "pandadb.db")
  }

  private def getValueWithDefault[T](key: String, defaultValue: () => T, convert: (String) => T)(implicit m: Manifest[T]): T = {
    val opt = settingsMap.get(key);
    if (opt.isEmpty) {
      val value = defaultValue();
      logger.debug(s"no value set for $key, using default: $value");
      value;
    }
    else {
      val value = opt.get;
      try {
        convert(value);
      }
      catch {
        case e: java.lang.IllegalArgumentException =>
          throw new WrongArgumentException(key, value, m.runtimeClass);
      }
    }
  }

  private def getValueAsString(key: String, defaultValue: String): String =
    getValueWithDefault(key, () => defaultValue, (x: String) => x)

  private def getValueAsClass(key: String, defaultValue: Class[_]): Class[_] =
    getValueWithDefault(key, () => defaultValue, (x: String) => Class.forName(x))

  private def getValueAsInt(key: String, defaultValue: Int): Int =
    getValueWithDefault[Int](key, () => defaultValue, (x: String) => x.toInt)

  private def getValueAsBoolean(key: String, defaultValue: Boolean): Boolean =
    getValueWithDefault[Boolean](key, () => defaultValue, (x: String) => x.toBoolean)

}


class ArgumentRequiredException(key: String) extends
  RuntimeException(s"argument required: $key") {

}

class WrongArgumentException(key: String, value: String, clazz: Class[_]) extends
  RuntimeException(s"wrong argument: $key, value=$value, expected: $clazz") {

}
