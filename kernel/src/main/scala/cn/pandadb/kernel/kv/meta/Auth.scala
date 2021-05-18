package cn.pandadb.kernel.kv.meta

import cn.pandadb.kernel.kv.{ByteUtils, RocksDBStorage}
import cn.pandadb.kernel.kv.db.KeyValueDB


/**
 * @ClassName Auth
 * @Description TODO
 * @Author huchuan
 * @Date 2021/1/19
 * @Version 0.1
 */
class Auth (path: String, rocksdbConfPath:String = "defalut") {

  val db: KeyValueDB = RocksDBStorage.getDB(s"${path}/auth", rocksdbConfigPath = rocksdbConfPath)._1

  private val DEFAULT_USERNAME = "pandadb"
  private val DEFAULT_PASSWORD = "pandadb"
  private val FLAG_IS_NEW_USER: Array[Byte] = Array(-1.toByte, 1.toByte)

  private var _isInit: Boolean = _get(FLAG_IS_NEW_USER).forall(v=> ByteUtils.getBoolean(v, 0))

  db.put(FLAG_IS_NEW_USER, ByteUtils.booleanToBytes(_isInit))
  if(_isInit) add(DEFAULT_USERNAME, DEFAULT_PASSWORD)

  /**
   * Single Column Index:
   * ╔═══════════╦══════════╗
   * ║    key    ║   value  ║
   * ╠═══════════╬══════════╣
   * ║  username ║ password ║
   * ╚═══════════╩══════════╝
   */

  def _get(key: Array[Byte]): Option[Array[Byte]] =
    Option(db.get(key))

  def add(username: String, password: String): Boolean =
    get(username).orElse{
      db.put(username.getBytes, password.getBytes)
      None
    }.forall(_ => false)

  def set(username: String, password: String): Boolean = {
    if (_isInit && username.equals(DEFAULT_USERNAME)){
      _isInit = false
      db.put(FLAG_IS_NEW_USER, ByteUtils.booleanToBytes(_isInit))
    }
    get(username).exists {
      n =>
        db.put(n.getBytes, password.getBytes)
        true
    }
  }

  def get(username: String): Option[String] =
    _get(username.getBytes).map(ByteUtils.stringFromBytes(_))


  def delete(username: String): Boolean =
    get(username).exists {
      n =>
        db.delete(username.getBytes)
        true
    }

  def all(): Iterator[(String, String)] = {
    val iter = db.newIterator()
    iter.seekToFirst()
    new Iterator[(String, String)]{
      override def hasNext: Boolean = iter.isValid && !iter.key().startsWith(Array(-1.toByte))
      override def next(): (String, String) = {
        val key = iter.key()
        val value = iter.value()
        iter.next()
        (ByteUtils.stringFromBytes(key), ByteUtils.stringFromBytes(value))
      }
    }
  }

  def isDefault: Boolean = _isInit

  def check(username: String, password: String): Boolean =
    get(username).exists(password.equals)
}
