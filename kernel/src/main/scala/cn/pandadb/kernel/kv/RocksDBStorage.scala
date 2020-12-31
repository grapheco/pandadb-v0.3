package cn.pandadb.kernel.kv

import java.io.File

import org.rocksdb.{CompactionOptionsUniversal, Options, RocksDB}

object RocksDBStorage {
  RocksDB.loadLibrary()

  def getDB(path:String, createIfMissing:Boolean=true): RocksDB = {
    val options: Options = new Options().setCreateIfMissing(createIfMissing)
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException("Invalid db path, it's a regular file: " + path)

    RocksDB.open(options, path)
  }

  def getInitDB(path:String = "testdata/rocks/db", createIfMissing:Boolean=true): RocksDB = {
    val options: Options = new Options().setCreateIfMissing(createIfMissing)
      .setAllowConcurrentMemtableWrite(true).setMaxBackgroundFlushes(1)
      .setWriteBufferSize(256*1024*1024).setMaxWriteBufferNumber(8).setMinWriteBufferNumberToMerge(4)
      .setArenaBlockSize(512*1024)
      .setLevel0FileNumCompactionTrigger(512).setDisableAutoCompactions(true)
      .setMaxBackgroundCompactions(8)

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

