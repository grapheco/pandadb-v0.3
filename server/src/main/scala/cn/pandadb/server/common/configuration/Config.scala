package cn.pandadb.server.common.configuration

import java.io.{File, FileInputStream}
import java.util.Properties
import scala.collection.mutable
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging

object SettingKeys {
  val version = "db.version"
  val rpcListenHost = "dbms.server.rpc.listen.host"
  val rpcListenPort = "dbms.server.rpc.listen.port"
  val rpcServerName = "dbms.server.rpc.service.name"
  val dataRpcEndpointName = "dbms.server.rpc.data.endpoint"

  val localDBHomePath = "db.home.path"
  val localDBName = "db.name"
  val defaultLocalDBHome = "db.default.home.path" // set by starter script

  val rocksdbConfigPath = "db.rocksdb.file.path"

  // db path
  val nodeMetaDBPath = "db.nodeMetaDB.path"
  val nodeDBPath = "db.nodeDB.path"
  val nodeLabelDBPath = "db.nodeLabelDB.path"
  val relationMetaDBPath = "db.relationMetaDB.path"
  val relationDBPath = "db.relationDB.path"
  val inRelationDBPath = "db.inRelationDB.path"
  val outRelationDBPath = "db.outRelationDB.path"
  val relationLabelDBPath = "db.relationLabelDB.path"
  val statisticsDBPath = "db.statisticsDB.path"
  val indexDBPath = "db.indexDB.path"
  val indexMetaDBPath = "db.indexMetaDB.path"
  val fullIndexDBPath = "db.fullIndexDB.path"
  val authDBPath = "db.authDB.path"

}

class Config extends LazyLogging {
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

  def getNodeMetaDBPath(): String = {
    getValueAsString(SettingKeys.nodeMetaDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/nodeMeta")
  }
  def getNodeDBPath(): String = {
    getValueAsString(SettingKeys.nodeDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/nodes")
  }
  def getNodeLabelDBPath(): String = {
    getValueAsString(SettingKeys.nodeLabelDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/nodeLabel")
  }
  def getRelationMetaDBPath(): String = {
    getValueAsString(SettingKeys.relationMetaDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/relationMeta")
  }
  def getRelationDBPath(): String = {
    getValueAsString(SettingKeys.relationDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/rels")
  }
  def getInRelationDBPath(): String = {
    getValueAsString(SettingKeys.inRelationDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/inEdge")
  }
  def getOutRelationDBPath(): String = {
    getValueAsString(SettingKeys.outRelationDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/outEdge")
  }
  def getRelationLabelDBPath(): String = {
    getValueAsString(SettingKeys.relationLabelDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/relLabelIndex")
  }
  def getStatisticsDBPath(): String = {
    getValueAsString(SettingKeys.statisticsDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/statistics")
  }
  def getIndexDBPath(): String = {
    getValueAsString(SettingKeys.indexDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/index")
  }
  def getIndexMetaDBPath(): String = {
    getValueAsString(SettingKeys.indexMetaDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/indexMeta")
  }
  def getFullIndexDBPath(): String = {
    getValueAsString(SettingKeys.fullIndexDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/fulltextIndex")
  }
  def getAuthDBPath(): String = {
    getValueAsString(SettingKeys.authDBPath, s"${getLocalDataStorePath()}/${getLocalDBName()}/auth")
  }


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
    getValueAsString(SettingKeys.localDBHomePath, getDefaultDBHome() + "/data")
  }

  def getLocalDBName(): String ={
    getValueAsString(SettingKeys.localDBName, defaultValue = "pandadb.db")
  }

  def getDefaultDBHome(): String ={
    // startup script will set this value
    settingsMap(SettingKeys.defaultLocalDBHome)
  }
  def getRocksdbConfigFilePath(): String = {
    getValueAsString(SettingKeys.rocksdbConfigPath, defaultValue = "default")
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
