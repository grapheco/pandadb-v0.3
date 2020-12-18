package cn.pandadb.kernel.kv

import java.io.File

import org.rocksdb.{Options, RocksDB}

object RocksDBStorage {
  RocksDB.loadLibrary()

  def getDB(path:String = "testdata/rocks/db", createIfMissing:Boolean=true): RocksDB = {
    val options: Options = new Options().setCreateIfMissing(createIfMissing)
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    RocksDB.open(options, path)
  }
}

class RocksDBStorage(path: String) {
  val db: RocksDB = RocksDBStorage.getDB(path)
  def close(): Unit = ???

}

